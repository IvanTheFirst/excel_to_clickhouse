import pandas as pd
import numpy as np
import logging
from clickhouse_driver import Client
import time
import datetime
import os
import mailbox
from argparse import ArgumentParser
import yaml
import sys


logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)s - %(funcName)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

class click_connector_parser():
    def __init__(self,
                 loglevel,
                 dbsecure,
                 dbhost,
                 dbuser,
                 dbpass,
                 ttl,
                 table_engine,
                 reconnect_timeout_sec:int,
                 chcluster: str = ''
                 ):
        logger.setLevel(loglevel)
        self.dbsecure = dbsecure
        self.dbhost = dbhost
        self.dbuser = dbuser
        self.dbpass = dbpass
        self.ttl = ttl
        self.table_engine = table_engine
        self.chcluster = chcluster
        self.reconnect_timeout_sec = reconnect_timeout_sec
        self.db_scheme = {}

    def connect_to_db(self):
        click_connection_status = False
        while not click_connection_status:
            try:
                [host, port] = self.dbhost.split(':')
                self.click_client = Client(user=self.dbuser, password=self.dbpass,
                                           host=host, port=port, secure=self.dbsecure,
                                           settings={"use_numpy":True})
                result = self.click_client.execute('select 1')
                click_connection_status = True
                logger.info(f'Connected to DB {self.dbhost}')
            except Exception as e:
                logger.error(f"Can't connect to database {str(e)}")
                click_connection_status = False
                time.sleep(self.reconnect_timeout_sec)

    def check_connection(self) -> bool:
        try:
            result = self.click_client.execute('select 1')
            return True
        except:
            return False

    def get_db_scheme(self,database_name):
        query = f'SHOW TABLES FROM `{database_name}`'
        result = self.click_client.execute(query)
        tables = []
        for table in result:
            tables.append(table[0])
        dict_tables = {}
        for table in tables:
            if table not in dict_tables.keys():
                dict_tables[table] = []
            query = f'describe table `{database_name}`.`{table}`'
            result = self.click_client.execute(query)
            dict_tables[table] = []
            for row in result:
                dict_tables[table].append(row[0])
        self.db_scheme = dict_tables.copy()
        logger.info(f'got database {database_name} schema ')

    def schema_inserts(self,elements,database_name,table_name,timestamp_field_name:str=None):
        if not timestamp_field_name:
            timestamp_field_name = 'timestamp'
        if isinstance(elements, pd.DataFrame):
            df = elements
        else:
            df = pd.DataFrame(elements)
        types = {}
        for col_name in df:
            if df.dtypes[col_name] == object:
                types[col_name] = 'LowCardinality(String) CODEC(ZSTD)'
            elif df.dtypes[col_name] == bool:
                df[col_name] = df[col_name].replace({True: 1, False: 0})
                types[col_name] = 'UInt8'
            elif df.dtypes[col_name] == np.dtype('datetime64[ns]'):
                types[col_name] = 'DateTime64 CODEC(DoubleDelta,ZSTD)'
            elif df.dtypes[col_name] == np.half:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.float_:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.float64:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.double:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.single:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.float16:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.float32:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            # deprecated DeprecationWarning: `np.int0` is a deprecated alias for `np.intp`.  (Deprecated NumPy 1.24)
            # elif df.dtypes[col_name] == np.int0:
            #     types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.int16:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.int32:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.int64:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.int8:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.int_:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.intc:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.integer:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.integer:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.intp:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.signedinteger:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.short:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.longlong:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uint:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uint0:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uint16:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uint32:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uint64:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uint8:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uintc:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.uintp:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.unsignedinteger:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.ushort:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            elif df.dtypes[col_name] == np.ulonglong:
                types[col_name] = 'Float64 CODEC(Gorilla,ZSTD)'
            else:
                types[col_name] = 'String'
        schema_inserts = []
        if table_name not in self.db_scheme.keys():
            create_table = pd.io.sql.get_schema(df, table_name, dtype=types) + str("\n".join([
                f"ENGINE = {self.table_engine} ORDER BY {timestamp_field_name}",
                f"TTL toDateTime({timestamp_field_name}) + INTERVAL {self.ttl} DAY DELETE"]))
            create_table = create_table.replace('CREATE TABLE ', f'CREATE TABLE IF NOT EXISTS {database_name}.')
            if self.chcluster:
                create_table = create_table[:create_table.find('(')] + f' on cluster {self.chcluster} (' + create_table[
                                                                                                       create_table.find(
                                                                                                           '(') + 1:]
            else:
                create_table = create_table[:create_table.find('(')] + '(' + create_table[ create_table.find('(') + 1:]
            logger.debug(create_table)
            schema_inserts.append(create_table)
            self.db_scheme[table_name] = list(types.keys())
        else:
            columns_to_add = list(set(df.columns) - set(self.db_scheme[table_name]))
            if len(columns_to_add) > 0:
                for colunm in columns_to_add:
                    self.db_scheme[table_name].append(colunm)
                    if self.chcluster:
                        schema_inserts.append(
                            f"""ALTER TABLE `{database_name}`.`{table_name}` ON CLUSTER {self.chcluster} ADD COLUMN IF NOT EXISTS `{colunm}` {types[colunm]} """)
                    else:
                        schema_inserts.append(
                            f"""ALTER TABLE `{database_name}`.`{table_name}` ADD COLUMN IF NOT EXISTS `{colunm}` {types[colunm]} """)
        return schema_inserts

def extractattachements(message,tempdir,suffix=None)->list:
    filenames = []
    if message.get_content_maintype() == 'multipart':
        for part in message.walk():
            if part.get_content_maintype() == 'multipart': continue
            #if part.get('Content-Disposition') is None: continue
            if part.get('Content-Type').find('application/octet-stream') == -1: continue
            filename = part.get_filename()
            if suffix:
                filename = ''.join( [filename.split('.')[0], '_', suffix, '.', filename.split('.')[1]])
            filename = os.path.join(tempdir, filename)
            fb = open(filename,'wb')
            fb.write(part.get_payload(decode=True))
            fb.close()
            filenames.append(filename)
    return filenames

def files_from_imap_maildir(maildir,tempdir,mail_subject_to_search)->list:
    mbox = mailbox.Maildir(maildir)
    files_to_load = list()
    for email_item in mbox.items():
        if not (email_item[1]['Subject'].find('RvTools') != -1):
            continue
        logger.info('Subject: ' + email_item[1]['Subject'])
        index_of_comma_in_Date = email_item[1]['Date'].find(',')
        if index_of_comma_in_Date != -1:
            email_item[1]['Date'] = email_item[1]['Date'][index_of_comma_in_Date + 2:]
        # if Date is "1 Jun 2018" instead of "01 Jun 2018"
        date_list = email_item[1]['Date'].split()
        new_day = date_list[0]
        if len(date_list[0]) == 1:
            new_day = ''.join(['0', date_list[0]])
        new_date = ' '.join([new_day, date_list[1], date_list[2]])
        suffix = datetime.datetime.strptime(new_date, '%d %b %Y').strftime('%Y%m%d')
        email_msg = mbox.get(email_item[0])
        for header in email_msg._headers:
            # if header[0] == 'Date':
            #    if header[1].find(',') != -1:
            #        header = 'Date',header[1][header[1].find(',')+2:]
            #    #print(header)
            #    if len(header[1].split()[0]) == 1:
            #        new_day = ''.join(['0', header[1].split()[0]])
            #    else:
            #        new_day = header[1].split()[0]
            #        #print('new_day',new_day)
            #        new_date = ' '.join([new_day, header[1].split()[1], header[1].split()[2]])
            #        #print('new_date',new_date)
            #        suffix = datetime.datetime.strptime(new_date, '%d %b %Y').strftime('%Y%m%d')
            if header[0] == 'Subject' and header[1].find(mail_subject_to_search) != -1:
                files_to_load = files_to_load + extractattachements(email_msg,tempdir)
                mbox.remove(email_item[0])
    return files_to_load

def search_for_dates_and_time(string:str) -> datetime.datetime:
    templates = ['%Y-%m-%d %H:%M:%S %z', '%b %d %H:%M:%S', '%Y-%m-%d-%H-%M-%S']
    date_time_column_indexes = []
    timestamp_from_filename = str()
    for template in templates:
        date_string_len = len(time.strftime(template))
        for i in range(len(string) - date_string_len):
            try:
                timestamp_from_filename = datetime.datetime.strptime(string[i:i + date_string_len], template)
                date_time_column_indexes.append([i, i + date_string_len, 'datetime'])
            except:
                pass
    return timestamp_from_filename

class Configuration:
    def __init__(self):
        self.DEFAULT_CONFIG_PATH = "excel_to_clickhouse_conf.yaml"
        self.SAMPLE_CONFIG = """
## DEBUG, INFO, WARNING, ERROR, CRITICAL, default NOTSET
log_level: "INFO"

input:
    source_dir = ./

output:
    clickhouse:
        ## clickhouse connection scheme secure True/False
        secure: True
        ## clickhouse host and port
        host: "zonmondb.corp.tander.ru:9440"
        login: "login"
        password: "password"
        ## clickhouse cluster to create tables, leave empty or none for local database
        # cluster: "monitoring"
        ttl: 720
        ## ReplicatedMergeTree - for cluster, MergeTree for local
        table_engine: "MergeTree"
        database_name: "cmdb_some_conf"
        reconnect_timeout_sec: 60
"""

    def create_config(self, create_config_path):
        f = open(create_config_path, mode="w")
        f.write(self.SAMPLE_CONFIG)
        f.close()

    def read(self) -> dict:
        parser = ArgumentParser()
        parser.add_argument("-c", "--config", dest="config",
                            help="path to configuration file",
                            required=False,
                            default=self.DEFAULT_CONFIG_PATH)
        parser.add_argument("--create_config", dest="create_config",
                            help=f"Create default config {self.DEFAULT_CONFIG_PATH}",
                            default='-1',
                            required=False)
        args = parser.parse_args()
        configuration_file = vars(args)['config']
        create_config_path = vars(args)['create_config']
        if create_config_path != '-1':
            self.create_config(create_config_path)
            sys.exit()
        with open(configuration_file, 'r') as f:
            output = yaml.safe_load(f)
        for key in vars(args).keys():
            output[key] = vars(args)[key]
        return output

def main():
    config = Configuration().read()
    if not config['input']['source_dir']:
        logger.error(f'input.source_dir not set in configuration file')
        sys.exit(1)
    logger.setLevel(config['log_level'])
    files_to_load = []
    try:
        files_to_load = [f for f in os.listdir(config['input']['source_dir']) if (os.path.isfile(f) and
                                                                                  (f.lower().endswith('.xls')
                                                                                   or f.lower().endswith('.xlsx')))]
    except Exception as e:
        logger.error("Can't read excel files from " + str(config['input']['source_dir']) + str(e))
        sys.exit(2)
    logger.debug('excel files to load ' + str(files_to_load))
    if len(files_to_load) == 0:
        logger.error("There is no *.xls or *.xlsx files from " + str(config['input']['source_dir']))
        sys.exit(3)
    logger.info('Connect to database')
    click_conf = config['output']['clickhouse']
    if 'cluster' not in click_conf.keys():
        click_conf['cluster'] = ''
    click_conn = click_connector_parser(loglevel=config['log_level'],
                              dbsecure=click_conf['secure'],
                              dbhost=click_conf['host'],
                              dbuser=click_conf['login'],
                              dbpass=click_conf['password'],
                              ttl=click_conf['ttl'],
                              table_engine=click_conf['table_engine'],
                              chcluster=click_conf['cluster'],
                              reconnect_timeout_sec=60)
    click_conn.connect_to_db()
    click_conn.get_db_scheme(click_conf['database_name'])

    for file in files_to_load:
        logger.info('Processing file:' + file)
        timestamp_from_file = search_for_dates_and_time(file)
        if not timestamp_from_file:
            timestamp_from_file = datetime.datetime.now()
        excel_file = pd.read_excel(file, sheet_name=None )
        for key_table in excel_file.keys():
            df = excel_file[key_table].copy(deep=True)
            df['timestamp'] = timestamp_from_file
            schema_inserts = click_conn.schema_inserts(df,database_name=click_conf['database_name'],table_name=key_table)
            if len(schema_inserts) > 0:
                try:
                    for insert in schema_inserts:
                        click_conn.click_client.execute(insert)
                except Exception as e:
                    logger.warning(f"creating schema error {str(e)}")
            try:
                result = click_conn.click_client.insert_dataframe(f"INSERT INTO `{click_conf['database_name']}`.`{key_table}` VALUES ",df)
            except Exception as e:
                # SOCKET_TIMEOUT = 209
                # NETWORK_ERROR = 210
                error_code = ""
                try:
                    error_code = str(e).split()[1].replace('Code:', '')[:-1]
                except:
                    pass
                if error_code in ['209', '210']:
                    logger.error(f"Network connection to database error {str(e)}. Reconnect ather 1 minute")
                    time.sleep(60)
                    click_conn.connect_to_db()
                else:
                    logger.warning(f"writting file {file} error {str(e)}")
    pass

if __name__ == "__main__":
    main()
