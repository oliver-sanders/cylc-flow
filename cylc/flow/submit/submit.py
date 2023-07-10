import asyncio
from contextlib import suppress
import os
import os.path
import re
import typing as ty
from weakref import WeakValueDictionary

from cylc.flow.cfgspec.glbl_cfg import glbl_cfg
from cylc.flow.parsec.util import (
    pdeepcopy,
    poverride,
)
from cylc.flow.platforms import (
    HOST_REC_COMMAND,
    PLATFORM_REC_COMMAND,
    platform_name_from_job_info2,
)
from cylc.flow.remote import (
    is_remote_host,
)
from cylc.flow.submit.remote import (
    remote_init as _remote_init,
    remote_file_install as _remote_file_install,
)


class SelectCache():

    def __init__(self):
        self.remote_init_cache = {}
        self.remote_file_install_cache = {}
        self.host_selection_batches = WeakValueDictionary()
        self.job_submission_queue = {}
        self.platforms = glbl_cfg().get(['platforms']),


async def remote_init(cache, platform, host):
    with suppress(KeyError):
        return await cache.remote_init_cache[platform['install target']]
    coro = _remote_init(platform, host)
    cache.remote_init_cache[platform['install target']] = coro
    return await coro


async def remote_file_install(cache, platform, host):
    with suppress(KeyError):
        return await cache.remote_file_install_cache[
            platform['install target']
        ]
    coro = _remote_file_install(platform, host)
    cache.remote_file_install_cache[platform['install target']] = coro
    return await coro


async def submit_job(cache, itask, platform, host):
    """Run the job submission command."""
    key = (platform['name'], host)
    with suppress(KeyError):
        cache.job_submission_queue.append(itask)
        return await cache.job_submission_queue[key]
    cache.job_submission_queue[key] = [itask]

    # wait for other submissions to queue up behind us
    await asyncio.sleep(0)

    # submit the batch
    _submit_jobs(cache.job_submission_queue, platform, host)


def _get_rtconfig(itask, broadcast_mgr):
    """Return the runtime config for a task."""
    # Handle broadcasts
    overrides = broadcast_mgr.get_broadcast(
        itask.tokens
    )
    if overrides:
        rtconfig = pdeepcopy(itask.tdef.rtconfig)
        poverride(rtconfig, overrides, prepend=True)
    else:
        rtconfig = itask.tdef.rtconfig
    return rtconfig


def _host_selector(platform_string, bad_hosts):
    """Yields hosts for a platform string.

    Note that platform_string could be a platform_group.

    """
    for platform in get_platform(platform_string):
        for host in get_host(platform, bad_hosts):
            yield (platform, host)


async def eval_host(host_str: str, subprocpool) -> ty.Optional[str]:
    """Evaluate a host from a possible subshell string.

    Args:
        host_str: An explicit host name, a command in back-tick or
            $(command) format, or an environment variable holding
            a hostname.

    Returns 'localhost' if evaluated name is equivalent
    (e.g. localhost4.localdomain4).
    """
    host = await subshell_eval(host_str, HOST_REC_COMMAND, subprocpool)
    if host is not None and not is_remote_host(host):
        return 'localhost'
    return host


async def eval_platform(platform_str: str, subprocpool) -> ty.Optional[str]:
    """Evaluate a platform from a possible subshell string.

    Args:
        platform_str: An explicit platform name, a command in $(command)
            format, or an environment variable holding a platform name.
    """
    return await subshell_eval(platform_str, PLATFORM_REC_COMMAND, subprocpool)


async def subshell_eval(
    eval_str: str,
    command_pattern: re.Pattern,
    subprocpool,
) -> ty.Optional[str]:
    """Evaluate a platform or host from a possible subshell string.

    Arguments:
        eval_str:
            An explicit host/platform name, a command, or an environment
            variable holding a host/patform name.
        command_pattern:
            A compiled regex pattern designed to match subshell strings.

    Return:
        - None if evaluation of command is still taking place.
        - 'localhost' if string is empty/not defined.
        - Otherwise, return the evaluated host/platform name on success.

    Raise PlatformError on error.

    """
    if not eval_str:
        return 'localhost'

    # Host selection command: $(command) or `command`
    match_ = command_pattern.match(eval_str)
    if match_:
        eval_str = await subprocpool.run(
            ['bash', '-c', match_.groups()[1]],
            env=dict(os.environ)
        )

    # Environment variable substitution
    return os.path.expandvars(eval_str)


async def _get_host_host_selector(cache, itask, rtconfig, bad_hosts, subprocpool):
    """"Return a generator which picks hosts a task could submit to."""
    # get the platform expression
    platform_expr = rtconfig['platform'] 
    host_expr = rtconfig['remote']['host']
    if platform_expr and host_expr:
        raise WorkflowConfigError(
            "A mixture of Cylc 7 (host) and Cylc 8 (platform)"
            " logic should not be used. In this case for the task "
            f"\"{itask.identity}\" the following are not compatible:\n"
        )

    # check whether we are currently submitting other jobs for this expression
    key = (platform_expr, host_expr)
    with suppress(KeyError):
        return cache.host_selection_batches[key]

    # evaludate the platform/host expression
    if host_expr:
        host_string = await eval_host(host_expr, subprocpool)
        platform_string = platform_name_from_job_info2(
            cache.platforms,
            host_string,
            rtconfig,
        )
    else:
        platform_string = await eval_platform(platform_expr, subprocpool)

    # return the host selector
    sel = _host_selector(platform_string, bad_hosts)
    # TODO: cache the PlatformError
    cache.host_selection_batches[key] = sel
    return sel


# def _group_by_platform(itasks):
#     ret = {}
#     for itask in itasks:
#         platform_string = itask.taskdef.rtconfig['platform']
#         ret.setdefault(platform_string, []).append(itask)
#     return ret


async def submitter(bad_hosts, submission_queue, subprocpool):
    """Job submitter thinggy.

    When you want jobs to be submitted, push the corresponding tasks
    into the submission_queue and lean back, the submitter does the work
    for you.
    """
    cache = SelectCache()
    while True:
        for itask in submission_queue.get()
            try:
                await _submit(cache, itask, bad_hosts, subprocpool)
            except JobSyntaxError:
                pass  # submit-fail
            except PlatformError:
                pass  # submit-fail
            except SubmissionError:
                pass  # submit-fail
            else:
                pass # submitted


async def _submit(cache, itask, bad_hosts, subprocpool):
    """The job submission pipeline for a single task."""
    rtconfig = _get_rtconfig(itask, broadcast_mgr)
    select_host = await _get_host_host_selector(cache, itask, rtconfig, bad_hosts, subprocpool)
    for platform, host in select_host:
        try:
            await check_syntax(itask, platform, host)
            await remote_init(cache, platform, host)
            await remote_file_install(cache, platform, host)
            await submit_job(cache, itask, platform, host)
            break
        except SSHError:
            # LOG.warning()
            bad_hosts.add(host)
            continue
    else:
        raise PlatformError(f'no hosts available for {platform}')
