import json
import logging
import os

from multiprocessing import Process

from flask import Flask, request
from flask_restful import Resource
from flask_cors import CORS
from waitress import serve

from internal.utils import check_email, get_log_file_path
from internal.submitter import run_task
from internal.recorder import get_num_processing_jobs, was_processed, get_job_status
from internal import consts

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})


@app.before_first_request
def before_first_request_func():
    # set up the log file before the first request
    directory = os.path.dirname(consts.LOG_FOLDER)
    if not os.path.exists(directory):
        os.makedirs(directory)
    log_file_name = os.path.join(directory, consts.LOG_FILE)
    # logging.basicConfig(filename=log_file_name, level=logging.INFO)

    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger('flask_cors').level = logging.INFO

@app.route('/', methods=['GET'])
def hello_world():
    return "Hello, World!"

@app.route("/task/<uuid:job_id>", methods=['POST'])
def task(job_id):

    # check not too many processing jobs
    number_process_running = get_num_processing_jobs()
    if number_process_running>=consts.MAX_NUM_CONCURRENT_PROCESSING_JOBS:
        return "Bad request, job_id % s declined, too many jobs are processing: %s" % (job_id, number_process_running), 401

    # check the valid input
    body_job_id = request.json.get("job_id")
    body_operator = request.json.get("operator")

    if body_job_id is None or body_operator is None or check_email(body_operator)==False:
        return "Bad request, invalid job_id: %s or operator: %s" % (body_job_id, body_operator), 401

    # check if the job was already processed
    if was_processed(request.json.get("job_id")):
        return "Bad rquest, job_id %s was already received, its current status is %s" % (body_job_id, body_operator)), 401

    bidding_cb = Process(target=run_task, args=(body_job_id, body_operator))
    bidding_cb.start()

    return "Job_id %s received! The number jobs are running: %s" % (body_job_id, number_process_running), 202

@app.route("/status/<uuid:job_id>", methods=['GET'])
def status(job_id):
    job_status = get_job_status(job_id)
    return job_status, 200

@app.route("/log_file/<uuid:job_id>", methods=['GET'])
def log_file(job_id):
    # check the job_id is running, waiting or submitted, if so send log file
    job_status = get_job_status(job_id)
    if job_status in (consts.COMPLETED_STATUS, consts.FAILED_STATUS, consts.WAITING_STATUS, consts.RUNNING_STATUS):
        file_path = get_log_file_path(str(job_id))
        with open(file_path, "r") as fin:
            log = fin.read()

        if log == "":
            return "Job %s is %s, but log file not found" % (job_id, job_status), 404
        return log, 201
    else:
        return "Job %s is still %s " % (job_id, job_status), 404


if __name__ == '__main__':
    # export FLASK_APP=bidding_api.py
    # flask run --host=0.0.0.0

    # production mode
    serve(app, port=6060)

    # debug mode
    # app.run(host='0.0.0.0', debug=False, port=6060)


#
