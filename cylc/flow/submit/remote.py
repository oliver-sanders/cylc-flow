from contextlib import suppress
import os.path
from pathlib import Path
import re
from shlex import quote
import tarfile
from tempfile import SpooledTemporaryFile
import typing as ty

from cylc.flow import LOG
from cylc.flow.exceptions import (
    PlatformError,
)
import cylc.flow.flags
from cylc.flow.network.client_factory import CommsMeth
from cylc.flow.option_parsers import verbosity_to_opts
from cylc.flow.pathutil import (
    get_dirs_to_symlink,
    get_remote_workflow_run_dir,
    get_workflow_file_install_log_dir,
    get_workflow_run_dir,
)
from cylc.flow.platforms import (
    get_localhost_install_target,
    log_platform_event,
)
from cylc.flow.remote import (
    construct_rsync_over_ssh_cmd,
    construct_ssh_cmd,
)
from cylc.flow.workflow_files import (
    KeyInfo,
    KeyOwner,
    KeyType,
    WorkflowFiles,
    get_contact_file_path,
    get_workflow_srv_dir,
)

from cylc.flow.loggingutil import get_next_log_number, get_sorted_logs_by_time

from cylc.flow.util import format_cmd


def _remote_init_items(
    workflow_id,
    comms_meth: CommsMeth,
) -> ty.List[ty.Tuple[str, str]]:
    """Return list of items to install based on communication method.

    (At the moment this is only the contact file.)

    Return (list):
        Each item is (source_path, dest_path) where:
        - source_path is the path to the source file to install.
        - dest_path is relative path under workflow run directory
          at target remote.
    """
    if comms_meth not in {CommsMeth.SSH, CommsMeth.ZMQ}:
        return []
    return [
        (
            get_contact_file_path(workflow_id),
            os.path.join(
                WorkflowFiles.Service.DIRNAME,
                WorkflowFiles.Service.CONTACT
            )
        )
    ]


async def remote_init(
    workflow_id: str,
    platform: ty.Dict[str, ty.Any],
    host: str,
    curve_auth: 'ThreadAuthenticator',
    client_pub_key_dir: str,
    subprocpool,
) -> bool:
    """Initialise a remote host if necessary.

    Call "cylc remote-init" to install workflow items to remote:
        ".service/contact": For TCP task communication
        "python/": if source exists

    Args:
        platform: A dict containing settings relating to platform used in
            this remote installation.
        curve_auth: The ZMQ authenticator.
        client_pub_key_dir: Client public key directory, used by the
            ZMQ authenticator.

    """
    install_target = platform['install target']
    if install_target == get_localhost_install_target():
        return True

    # Determine what items to install
    comms_meth: CommsMeth = CommsMeth(platform['communication method'])
    remote_init_items = _remote_init_items(workflow_id, comms_meth)

    # Create a TAR archive with the service files,
    # so they can be sent later via SSH's STDIN to the task remote.
    tmphandle = SpooledTemporaryFile()
    with tarfile.open(fileobj=tmphandle, mode='w') as tarhandle:
        for path, arcname in remote_init_items:
            tarhandle.add(path, arcname=arcname)
    tmphandle.seek(0)
    # Build the remote-init command to be run over ssh
    cmd = [
        'remote-init',
        *verbosity_to_opts(cylc.flow.flags.verbosity),
        str(install_target),
        get_remote_workflow_run_dir(workflow_id)
    ]
    dirs_to_symlink = get_dirs_to_symlink(install_target, workflow_id)
    for key, value in dirs_to_symlink.items():
        if value is not None:
            cmd.append(f"{key}={quote(value)} ")
    # Create the ssh command
    log_platform_event('remote init', platform, host)
    cmd = construct_ssh_cmd(cmd, platform, host)
    proc = await subprocpool.run(cmd)
    # self.proc_pool.put_command(
    #     SubProcContext(
    #         'remote-init',
    #         cmd,
    #         stdin_files=[tmphandle],
    #         host=host
    #     ),
    #     bad_hosts=self.bad_hosts,
    #     callback=self._remote_init_callback,
    #     callback_args=[
    #         platform, tmphandle, curve_auth, client_pub_key_dir
    #     ],
    #     callback_255=self._remote_init_callback_255,
    #     callback_255_args=[platform]
    # )

    # Callback when "cylc remote-init" exits.

    # Write public key for install target into client public key
    # directory.
    # Set remote_init__map status to REMOTE_INIT_DONE on success which
    # in turn will trigger file installation to start.
    # Set remote_init_map status to REMOTE_INIT_FAILED on error.

    with suppress(OSError):  # E.g. ignore bad unlink, etc
        tmphandle.close()
    install_target = platform['install target']
    if proc.ret_code == 0 and "KEYSTART" in proc.out:
        regex_result = re.search(
            'KEYSTART((.|\n|\r)*)KEYEND',
            proc.out,
        )
        key = regex_result.group(1)
        workflow_srv_dir = get_workflow_srv_dir(workflow_id)
        public_key = KeyInfo(
            KeyType.PUBLIC,
            KeyOwner.CLIENT,
            workflow_srv_dir=workflow_srv_dir,
            install_target=install_target
        )
        old_umask = os.umask(0o177)
        with open(
                public_key.full_key_path,
                'w', encoding='utf8') as text_file:
            text_file.write(key)
        os.umask(old_umask)
        # configure_curve must be called every time certificates are
        # added or removed, in order to update the Authenticator's
        # state.
        curve_auth.configure_curve(
            domain='*', location=(client_pub_key_dir))
        return True
    # Bad status
    LOG.error(
        PlatformError(
            PlatformError.MSG_INIT,
            platform['name'],
            cmd=proc.cmd,
            ret_code=proc.ret_code,
            out=proc.out,
            err=proc.err,
        )
    )
    return False

#     workflow_id,
#     platform: ty.Dict[str, ty.Any],
#     host: str,
#     curve_auth: 'ThreadAuthenticator',
#     client_pub_key_dir: str,
#     subprocpool,


async def file_install(
    workflow_id: str,
    platform: ty.Dict[str, ty.Any],
    host: str,
    rsync_includes,
    subprocpool,
) -> bool:
    """Install required files on the remote install target.

    Included by default in the file installation:
        Files:
            .service/server.key  (required for ZMQ authentication)
        Directories:
            app/
            bin/
            etc/
            lib/
    """
    install_target = platform['install target']
    src_path = get_workflow_run_dir(workflow_id)
    dst_path = get_remote_workflow_run_dir(workflow_id)
    install_target = platform['install target']
    cmd, _host = construct_rsync_over_ssh_cmd(
        src_path,
        dst_path,
        platform,
        rsync_includes,
        dst_host=host,
    )
    log_platform_event('remote file install', platform, host)
    proc = await subprocpool.run(cmd)

    # Callback when file installation exits.

    # Sets remote_init_map to REMOTE_FILE_INSTALL_DONE on success and to
    # REMOTE_FILE_INSTALL_FAILED on error.

    install_log_dir = get_workflow_file_install_log_dir(workflow_id)
    file_name = _get_log_file_name(
        install_target, install_log_dir
    )
    install_log_path = get_workflow_file_install_log_dir(
        workflow_id,
        file_name,
    )

    out, err = proc.communicate()

    if out:
        Path(install_log_path).parent.mkdir(parents=True, exist_ok=True)
        with open(install_log_path, 'a') as install_log:
            install_log.write(
                f'$ {format_cmd(cmd, maxlen=80)}'
                '\n\n### STDOUT:'
                f'\n{out}'
            )
            if err:
                install_log.write(
                    '\n\n### STDERR:'
                    f'\n{err}'
                )
    if proc.ret_code == 0:
        # Both file installation and remote init success
        log_platform_event('remote file install complete', platform)
        return True

    LOG.error(
        PlatformError(
            PlatformError.MSG_INIT,
            platform['name'],
            # ctx=ctx,
        )
    )
    return False


def _get_log_file_name(
    install_target,
    install_log_dir,
    log_file_prefix: str = 'start',
):
    log_files = get_sorted_logs_by_time(install_log_dir, '*.log')
    log_num = get_next_log_number(log_files[-1]) if log_files else 1
    file_name = f"{log_num:02d}-{log_file_prefix}-{install_target}.log"
    return file_name
