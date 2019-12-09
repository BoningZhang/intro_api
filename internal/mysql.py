
import MySQLdb
import logging
import subprocess

class MySQLdb_wrapper:
    def __init__(self, host, user, pw, db_name, connect_timeout=28800, port=3306):
        self.host = host
        self.user = user
        self.pw = pw
        self.db_name = db_name
        self.connect_timeout = connect_timeout
        self.port = port

    def connect(self):
        try:
            db =  MySQLdb.connect(
                host=self.host,
                user=self.user,
                passwd=self.pw,
                db=self.db_name,
                connect_timeout=self.connect_timeout,
                port=self.port
            )
            return db
        except:
            logging.error("mysql database connection fails!")


    def write_query(self, sql):
        logging.info(sql)
        self.writeDb = self.connect()
        cursor = self.writeDb.cursor()
        try:
            cursor.execute(sql)
        except:
            logging.error("sql query execution fails: %s" % sql)
        cursor.close()
        self.writeDb.commit()
        self.writeDb.close()

    def run_query_output(self, sql):
        self.writeDb = self.connect()
        cursor = self.writeDb.cursor()
        try:
            cursor.execute(sql)
        except:
            logging.error("sql query execution fails: %s" % sql)

        myresult = cursor.fetchall()
        cursor.close()
        self.writeDb.commit()
        self.writeDb.close()
        return myresult

    def sql_quote_col(self, val):
        if type(val) is str:
            return "\"%s\"" % val
        elif val is None:
            return "NULL"
        else:
            return "%s" % str(val)

    def insert_row(self, table_name, row_obj):

        keys = row_obj.keys()
        values = map(lambda v: self.sql_quote_col(row_obj[v]), keys)
        update_values = map(lambda v: "%s=VALUES(%s)" % (v, v), keys)

        sql = """INSERT INTO {table_name} ({column_names}) VALUES ({column_values}) ON DUPLICATE KEY UPDATE {update_values};""".format(
                table_name=table_name,
                column_names=", ".join(keys),
                column_values=", ".join(values),
                update_values=", ".join(update_values)
            )
        logging.debug(sql)
        self.write_query(sql)

    def insert_rows(self, table_name, row_obj_list):
        for row_obj in row_obj_list:
            self.insert_row(table_name, row_obj)

    def load_file_to_mysql(self, local_file_dir, mysql_table):
        sql = """
            LOAD DATA CONCURRENT local INFILE '{local_file_dir_sql}'
            INTO TABLE {mysql_table_sql} FIELDS TERMINATED BY X'01';commit;
        """.format(local_file_dir_sql=local_file_dir, mysql_table_sql=mysql_table)
        self.write_query(sql)

        cmd = ["rm", local_file_dir]
        logging.info(cmd)
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        except:
            logging.error("system command fails: %s" % cmd)
        out, err = p.communicate()
        logging.info(out)
