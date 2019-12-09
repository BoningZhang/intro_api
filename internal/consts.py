# consts
# max number of concurrent jobs
MAX_NUM_CONCURRENT_PROCESSING_JOBS = 8
MAX_NUM_CONCURRENT_RUNNING_JOBS = 4

MYSQL_INFO = {
    "sql_host": "host_ip",
    "sql_uname": "username",
    "sql_password": "password",
    "sql_db": "database_name"
}
MYSQL_TABLE = "job_status_tracker"

# job status
EDITTING_STATUS = "editing"
RUNNING_STATUS = "running"
WAITING_STATUS = "waiting"
COMPLETED_STATUS = "completed"
FAILED_STATUS = "failed"

SLEEPING_SECONDS = 30

# api log folder
LOG_FOLDER = "/mnt/bidding_api/bms_logs"
LOG_FILE_PATTERN = "bidding_job_%s.log"

HOST_IP="HOST_IP:6060/"
