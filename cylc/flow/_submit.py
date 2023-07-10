# async def submit(itask, bad_hosts):
#     platform_string = itask.taskdef.rtconfig['platform']
#     for platform in select_platform(platform_string):
#         for host in select_host(platform, bad_hosts):
#             ret = await remote_init(platform, host)
#             if ret.return_code == 255:
#                 bad_hosts.add(host)
#                 continue
#             ret = await remote_fileinstall(platform, host)
#             if ret.return_code == 255:
#                 bad_hosts.add(host)
#                 continue
#             await check_syntax(itask, platform, host)
#             ret = await submit_job(itask, platform, host)
#             if ret.return_code == 255:
#                 bad_hosts.add(host)
#                 continue
#             break
#     else:
#         raise PlatformError(f'no hosts available for {platform}')

from contextlib import suppress
from weakref import WeakValueDictionary


class SelectCache():

    def __init__(self):
        self.remote_init_cache = {}
        self.remote_file_install_cache = {}
        self.host_selection_batches = WeakValueDictionary()


async def remote_init(cache, platform, host):
    with suppress(KeyError):
        return RI_MAP[platform['install target']]
    await _remote_init(platform, host)


async def remote_fileinstall(cache, platform, host):
    with suppress(KeyError):
        return RF_MAP[platform['install target']]
    await _remote_fileinstall(platform, host)


def _selector(platform_string):
    pass


def _get_host_selector(cache, platform_string):
    with suppress(KeyError):
        return cache.host_selection_batches[platform_string]
    sel = _selector(platform_string)
    cache.host_selection_batches[platform_string] = sel
    return sel


def _group_by_platform(itasks):
    ret = {}
    for itask in itasks:
        platform_string = itask.taskdef.rtconfig['platform']
        ret.setdefault(platform_string, []).append(itask)
    return ret


async def submit(itasks, bad_hosts):
    for platform_string, tasks in _group_by_platform(itasks).items():
        select_host = _get_host_selector(platform_string)
        for itask in tasks:
            try:
                await _submit(itask, bad_hosts, select_host)
            except JobSyntaxError:
                pass  # submit-fail
            except PlatformError:
                pass  # submit-fail
            else:
                pass # submitted


async def _submit(itask, bad_hosts, select_host):
    for platform, host in select_host(bad_hosts):
        try:
            await check_syntax(itask, platform, host)
            await remote_init(platform, host)
            await remote_fileinstall(platform, host)
            await submit_job(itask, platform, host)
            break
        except SSHError:
            # LOG.warning()
            bad_hosts.add(host)
            continue
    else:
        raise PlatformError(f'no hosts available for {platform}')
