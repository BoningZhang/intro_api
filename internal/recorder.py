import logging
import json

from internal.mysql import *
from internal import consts
from flask import abort

def create_mysql(curr_mysql_info=consts.MYSQL_INFO):
    crm_mysql = MySQLdb_wrapper(
        curr_mysql_info['sql_host'],
        curr_mysql_info['sql_uname'],
        curr_mysql_info['sql_password'],
        curr_mysql_info['sql_db']
    )

    return crm_mysql

def get_num_processing_jobs():

    crm_mysql = create_mysql()
    sql = """
        SELECT COUNT(DISTINCT `uuid`)
        FROM {table_name_}
        WHERE job_status IN ("{running_}", "{waiting_}");
    """.format(table_name_=consts.MYSQL_TABLE, running_=consts.RUNNING_STATUS, waiting_=consts.WAITING_STATUS)
    num_out_orig = crm_mysql.run_query_output(sql)
    try:
        num_out = num_out_orig[0][0]
    except:
        logging.warn("Error in mysql query in get_num_processing_jobs()")
        abort(402)


    logging.info("Number of jobs are being processing: %s" % num_out)
    return num_out

def get_num_jobs(target_status):
    crm_mysql = create_mysql()
    sql = """
        SELECT COUNT(DISTINCT `uuid`)
        FROM {table_name_}
        WHERE job_status="{target_status_}";
    """.format(table_name_=consts.MYSQL_TABLE, target_status_=target_status)
    num_out_orig = crm_mysql.run_query_output(sql)
    try:
        num_out = num_out_orig[0][0]
    except:
        logging.warn("Error in mysql query in get_num_jobs()")
        abort(402)

    logging.info("Number of jobs %s: %s" % (target_status, num_out))
    return num_out

def get_job_status(job_id):
    crm_mysql = create_mysql()
    sql = """
        SELECT job_status
        FROM {table_name_}
        WHERE `uuid`="{job_id_}";
    """.format(table_name_=consts.MYSQL_TABLE, job_id_=job_id)
    status_out_orig = crm_mysql.run_query_output(sql)
    try:
        status_out = status_out_orig[0][0]
    except:
        logging.warn("Job_id %s not found in mysql database" % job_id)
        abort(402)

    logging.info("Status for job_id %s is %s" % (job_id, status_out))
    return status_out

def was_processed(job_id):
    job_status = get_job_status(job_id)

    return job_status in [consts.RUNNING_STATUS, consts.WAITING_STATUS, consts.COMPLETED_STATUS]

# update mysql table
def update_job_status(job_id, target_status):
    crm_mysql = create_mysql()

    mysql_dict = {"`uuid`": '"'+job_id+'"', "job_status": target_status}
    crm_mysql.insert_row(consts.MYSQL_TABLE, mysql_dict)


#
