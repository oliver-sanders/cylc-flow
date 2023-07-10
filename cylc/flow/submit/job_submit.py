def prep_submit_task_jobs(self, workflow, itasks, check_syntax=True):
    """Prepare task jobs for submit.

    Prepare tasks where possible. Ignore tasks that are waiting for host
    select command to complete. Bad host select command or error writing to
    a job file will cause a bad task - leading to submission failure.

    Return [list, list]: list of good tasks, list of bad tasks
    """
    prepared_tasks = []
    bad_tasks = []
    for itask in itasks:
        if not itask.state(TASK_STATUS_PREPARING):
            # bump the submit_num *before* resetting the state so that the
            # state transition message reflects the correct submit_num
            itask.submit_num += 1
            itask.state_reset(TASK_STATUS_PREPARING)
            self.data_store_mgr.delta_task_state(itask)
        prep_task = _prep_submit_task_job(
            workflow, itask, check_syntax=check_syntax)
        if prep_task:
            prepared_tasks.append(itask)
        elif prep_task is False:
            bad_tasks.append(itask)
    return [prepared_tasks, bad_tasks]


def _get_rtconfig(itask, broadcast_mgr):
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


def _prep_submit_task_job(
    self,
    workflow: str,
    itask: 'TaskProxy',
    check_syntax: bool = True
):
    """Prepare a task job submission.

    Returns:
        * itask - preparation complete.
        * None - preparation in progress.
        * False - perparation failed.

    """
    if itask.local_job_file_path:
        return itask

    rtconfig = _get_rtconfig(itask, broadcast_mgr)

    # BACK COMPAT: host logic
    # Determine task host or platform now, just before job submission,
    # because dynamic host/platform selection may be used.
    # cases:
    # - Platform exists, host does = throw error here:
    #    Although errors of this sort should ideally be caught on config
    #    load this cannot be done because inheritance may create conflicts
    #    which appear later. Although this error is also raised
    #    by the platforms module it's probably worth putting it here too
    #    to prevent trying to run the remote_host/platform_select logic for
    #    tasks which will fail anyway later.
    # - Platform exists, host doesn't = eval platform_name
    # - host exists - eval host_n
    # remove at:
    #     Cylc8.x

    # if (
    #     rtconfig['platform'] is not None and
    #     rtconfig['remote']['host'] is not None
    # ):
    #     raise WorkflowConfigError(
    #         "A mixture of Cylc 7 (host) and Cylc 8 (platform) "
    #         "logic should not be used. In this case for the task "
    #         f"\"{itask.identity}\" the following are not compatible:\n"
    #     )

    # host_n, platform_name = None, None
    # try:
    #     if rtconfig['remote']['host'] is not None:
    #         host_n = self.task_remote_mgr.eval_host(
    #             rtconfig['remote']['host']
    #         )
    #     else:
    #         platform_name = self.task_remote_mgr.eval_platform(
    #             rtconfig['platform']
    #         )
    except PlatformError as exc:
        itask.waiting_on_job_prep = False
        itask.summary['platforms_used'][itask.submit_num] = ''
        # Retry delays, needed for the try_num
        self._create_job_log_path(workflow, itask)
        self._set_retry_timers(itask, rtconfig)
        self._prep_submit_task_job_error(
            workflow, itask, '(remote host select)', exc
        )
        return False
    else:
        # host/platform select not ready
        if host_n is None and platform_name is None:
            return
        elif (
            host_n is None
            and rtconfig['platform']
            and rtconfig['platform'] != platform_name
        ):
            LOG.debug(
                f"for task {itask.identity}: platform = "
                f"{rtconfig['platform']} evaluated as {platform_name}"
            )
            rtconfig['platform'] = platform_name
        elif (
            platform_name is None
            and rtconfig['remote']['host'] != host_n
        ):
            LOG.debug(
                f"[{itask}] host = "
                f"{rtconfig['remote']['host']} evaluated as {host_n}"
            )
            rtconfig['remote']['host'] = host_n

        try:
            platform = get_platform(
                rtconfig, itask.tdef.name, bad_hosts=self.bad_hosts
            )
        except PlatformLookupError as exc:
            itask.waiting_on_job_prep = False
            itask.summary['platforms_used'][itask.submit_num] = ''
            # Retry delays, needed for the try_num
            self._create_job_log_path(workflow, itask)
            self._prep_submit_task_job_error(
                workflow, itask, '(platform not defined)', exc)
            return False
        else:
            itask.platform = platform
            # Retry delays, needed for the try_num
            self._set_retry_timers(itask, rtconfig)

    try:
        job_conf = {
            **self._prep_submit_task_job_impl(
                workflow, itask, rtconfig
            ),
            'logfiles': deepcopy(itask.summary['logfiles']),
        }
        itask.jobs.append(job_conf)

        local_job_file_path = get_task_job_job_log(
            workflow,
            itask.point,
            itask.tdef.name,
            itask.submit_num,
        )
        self.job_file_writer.write(
            local_job_file_path,
            job_conf,
            check_syntax=check_syntax,
        )
    except Exception as exc:
        # Could be a bad command template, IOError, etc
        itask.waiting_on_job_prep = False
        self._prep_submit_task_job_error(
            workflow, itask, '(prepare job file)', exc)
        return False

    itask.local_job_file_path = local_job_file_path
    return itask
