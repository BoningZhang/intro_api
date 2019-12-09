import logging
import time
import json
import os

from flask import abort

from internal.recorder import update_job_status, get_num_jobs
from internal import consts
from internal.utils import generate_log_file, append_log_file


def run_task(job_id, job_operator):
    # ready to process the job
    generate_log_file(job_id)

    update_job_status(job_id, consts.WAITING_STATUS)

    log_process = "Processing job_id: %s, job_operator: %s \n" % (job_id, job_operator)
    logging.info(log_process)
    append_log_file(job_id, log_process)

    # start to process the bidding
    while get_num_jobs(consts.RUNNING_STATUS)>consts.MAX_NUM_CONCURRENT_RUNNING_JOBS:
        # it is possible the later received jobs will jump out of this while
        # loop first than the earlier received jobs
        # Queue can be a solution to this, but then we need a background worker to
        # pick up the jobs in queue
        time.sleep(consts.SLEEPING_SECONDS)
        append_log_file(job_id, "There is still job running in this same bucket, Sleeping for %s seconds ... \n" % consts.SLEEPING_SECONDS)

    # there is extream case that two jobs are freed out from while loop
    # at the same instance

    # there is available worker to run this job
    # lock this worker for running this job
    append_log_file(job_id, "Running job_id: %s\n" % job_id)
    update_job_status(job_id, consts.RUNNING_STATUS)
    try:
        # simulate time expensive task
        time.sleep(500)

        # finish job running, unlock the worker
        update_job_status(job_id, consts.COMPLETED_STATUS)
        append_log_file(job_id, "Job was successfully submitted to BE...\n")
    except:
        update_job_status(job_id, consts.FAILED_STATUS)
        log_error = "Error: bidding job failed for job_id %s\n" % job_id
        append_log_file(job_id, log_error)
        logging.error(log_error)
        raise Exception(log_error)
        abort(403)

#
