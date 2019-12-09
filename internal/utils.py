import re
import logging
import os
import subprocess
import json

from internal import consts
from dateutil import tz

def check_email(email):
    # pass the regualar expression
    # and the string in search() method
    regex = '^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$'
    if(re.search(regex,email)):
        logging.info("Valid Email")
        return True
    else:
        logging.warn("Invalid Email")
        return False

def append_log_file(job_id, log_content):
    file_path = get_log_file_path(job_id)

    with open(file_path, "a") as fout:
        fout.write("\n"+log_content)

def get_log_file_path(job_id):
    file_name = consts.LOG_FILE_PATTERN % str(job_id).strip().replace("-", "_")
    file_path = os.path.join(consts.LOG_FOLDER, file_name)

    return file_path

def generate_log_file(job_id):
    file_path = get_log_file_path(job_id)

    with open(file_path, "w") as fout:
        fout.write("Job_id %s received! \n" % job_id)

#
