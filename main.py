# getting the list of all the financial statements to download from today as 'date'
# ONLY NASHER Companies (company type = 1 in search url)
import requests
from requests.adapters import HTTPAdapter, Retry
import jalali
from datetime import datetime, timedelta
import logging
import pandas as pd
from unidecode import unidecode
# from multiprocessing import Pool
# from itertools import repeat
import jalali_pandas
from pymongo.errors import BulkWriteError
from pymongo import MongoClient
import os
from utils import get_s3_client
from sync import sync_directory_to_s3
from dotenv import load_dotenv

load_dotenv()

today = datetime.now().date().strftime('%Y-%m-%d')
yesterday = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
path_download = today + '/fs-sheets'
path_logs = today + '/logs'
path_export = today + '/export'

os.makedirs(today, exist_ok=True)
os.makedirs(path_logs, exist_ok=True)
os.makedirs(path_download, exist_ok=True)
os.makedirs(path_export, exist_ok=True)

formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt = "%Y-%m-%d %H:%M")

# process logger
logger_p = logging.getLogger('process')
logger_p.setLevel(logging.INFO)
fh_process = logging.FileHandler(path_logs+"/process_errors.log", encoding='utf-8')
fh_process.setFormatter(formatter)
logger_p.addHandler(fh_process)

# download logger
logger_d = logging.getLogger('download')
logger_d.setLevel(logging.INFO)
fh_download = logging.FileHandler(path_logs+"/download_fs.log", encoding='utf-8')
fh_download.setFormatter(formatter)
logger_d.addHandler(fh_download)
from download import *
from process import *

# set main logger
logger = logging.getLogger('main')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(path_logs+"/updates.log", encoding='utf-8')
fh.setFormatter(formatter)
logger.addHandler(fh)

def get_list(date_j):
    n_page = 1
    dl_list = []
    # date_j = jalali.Gregorian(datetime.today().date()).persian_string("{}/{}/{}")
    se = requests.Session()
    retries = Retry(total=1,
                    backoff_factor=0.3,
                    status_forcelist=[ 500, 502, 503, 504 ])
    headers = {
        'Accept': 'application/json',   
        'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        # 'Cookie': 'Unknown=1076170924.20480.0000; TS018f30e4=01f9930bd25004a92d5c39c0a991dcecb352c8a7859cfbf02d8f452d460aa9aa3e775b07891903868ef6bbe2fa758aea712c1fb2952c636e6cdac208821a1b18709790f618823a3b5614466666c1dca443086d41dc365d1a79047ce194a79377841e54e1ce4b6260236ca1fcdd0cd3cac3bc0097b7; TS018fb0f7=01f9930bd2aec21c0723a2abc1fab3a6a7c7df63e6075a8ba0638ef854e7dabb2df5a2503de0c83d011ff921c40c2498d1cbcf408e',
    }

    se.mount('https://', HTTPAdapter(max_retries=retries))
    result = 1
    while result > 0: 
        try:
            search_url = f'https://search.codal.ir/api/search/v2/q?&LetterType=6&FromDate={date_j}&PageNumber={n_page}&CompanyType=1'
            response = se.get(url = search_url, headers=headers) 

            if response.status_code==200:
                result = len(response.json()['Letters'])
                if result > 0:
                    dl_list.extend(response.json()['Letters'])
            else:
                logger.info(f'[List Error] Response status {response.status_code}')
                result = 0
            n_page+=1
        except Exception as e_list_codal:
            logger.info(f'[List Error] Exception {e_list_codal}')
    
    return dl_list

class DateTimeDecoder(json.JSONDecoder):
    def __init__(self, datetime_keys=None, format="%Y-%m-%d %H:%M:%S", *args, **kwargs):
        self.datetime_keys = set(datetime_keys or [])
        self.format = format
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, obj):
        for key in self.datetime_keys:
            if key in obj and isinstance(obj[key], str):
                try:
                    obj[key] = datetime.strptime(obj[key], self.format)
                except ValueError:
                    logger.info('[Warning] Some dates were not correctly formatted.')
        return obj
    
def build_reprocess_list(path_log, list_df):
    with open(path_log, 'r') as plogs:
        errors = plogs.readlines()

    errors_tracing_no = [re.findall(pattern=r'report no\. \[(\d+)\]', string=f)[0] for f in errors]
    errors_df = pd.DataFrame(errors, columns=['er_str'])
    # errors_df['name'] = errors_df['er_str'].str.extract(r'\- \[(.*)\] \| report')
    errors_df['trace_no'] = errors_tracing_no

    # reprocess = errors_df[errors_df['er_str'].str.contains('duplicate')][['trace_no']].drop_duplicates()
    trace_no_errors = errors_df['trace_no'].unique()

    list_reprocess = list_df[list_df['trace_no'].astype(str).isin(trace_no_errors)]

    return list_reprocess
    
def update(date, path_download_html, path_download_excel, path_export, collection):
    date_j = jalali.Gregorian(datetime.strptime(date,'%Y-%m-%d').date()).persian_string("{}/{}/{}")
    logger.info(f'[Info] Starting to update for date: {date_j}')
    dl_list = get_list(date_j)
    if len(dl_list) == 0:
        logger.info(f'[Info] Found 0 financial statements.')
        logger.info('[Info] Update aborted.')
        return None
    df_list = pd.DataFrame(dl_list)
    df_list = df_list[['TracingNo', 'Symbol', 'CompanyName', 'Title', 'PublishDateTime', 'LetterCode', 'Url', 'ExcelUrl']]
    df_list.columns=['trace_no', 'symbol', 'company_name', 'title', 'date_j', 'letter_code', 'url', 'excel_url']
    df_list.loc[~df_list['url'].str.contains('https://codal.ir'), 'url'] = 'https://codal.ir' + df_list[~df_list['url'].str.contains('https://codal.ir')]['url']

    if 'sheet_no' not in df_list.columns:
        df_list['sheet_no'] = float('NaN')
    df_list['date_j'] = df_list['date_j'].apply(unidecode)
    df_list['date_g'] = df_list['date_j'].jalali.parse_jalali('%Y/%m/%d %H:%M:%S').jalali.to_gregorian()
    df_list['date_j'] = df_list['date_j'].str.slice(0,10)
    df_list = df_list[df_list['date_j'].eq(date_j)]
    logger.info(f'[Info] Found {len(df_list)} financial statements.')

    if len(df_list) == 0:
        logger.info('[Info] Update aborted.')
    
    else:
        download_df = df_list[['trace_no', 'url', 'sheet_no', 'excel_url']]

        # with Pool(processes=10) as pool:
        #     rows = [row for _, row in download_df.iterrows()]
        #     pool.starmap(download_sheets, zip(rows, repeat(path_download_html), repeat(path_download_excel)))
        for _, row in download_df.iterrows():
            download_sheets(row, path_download_html, path_download_excel)
        logger.info('[Info] Download has finished.')

        ref_df = df_list[['trace_no', 'symbol', 'company_name', 'title', 'date_j', 'date_g', 'url', 'excel_url']]#.astype(str)
        # rows = [row for _, row in ref_df.iterrows()]
        # with Pool(processes=5) as p:
        #     docs = p.starmap(json_export, zip(rows,  repeat(path_download_html), repeat(path_download_excel)))
        docs = []
        for _,row_ref in ref_df.iterrows():
            docs.append(json_export(row_ref, path_download_html, path_download_excel))
        with open(path_export+'/fs_success_data.json', 'w', encoding='utf-8') as uploadf:
            json.dump(docs, uploadf, cls=NpEncoder, ensure_ascii=False)
        processed_docs = json.loads(json.dumps(docs, cls=NpEncoder), cls=DateTimeDecoder, datetime_keys=['date_g'])
        logger.info('[Info] Processing has finished.')

        ## Find reports with error, redownload and reprocess ALL
        reprocess_df = build_reprocess_list(path_logs+"/process_errors.log", df_list)

        if len(reprocess_df) != 0:
            for _,r in reprocess_df.iterrows():
                download_sheets(r, path_download_html, path_download_excel)
            logger.info('[Info] Redownload reports with process error completed.')
            docs_reprocess = []
            for _, row_reprocess in ref_df.iterrows():
                docs_reprocess.append(json_export(row_reprocess, path_download_html, path_download_excel))
            with open(path_export+'/fs_success_data.json', 'w', encoding='utf-8') as uploadf:
                json.dump(docs_reprocess, uploadf, cls=NpEncoder, ensure_ascii=False)
            processed_docs = json.loads(json.dumps(docs_reprocess, cls=NpEncoder), cls=DateTimeDecoder, datetime_keys=['date_g'])

            logger.info('[Info] Finished reprocessing all reports.')

        try:
            result = collection.insert_many(processed_docs, ordered=False)
        except BulkWriteError as bwe:
            details = bwe.details
            write_errors = details.get("writeErrors", [])
            inserted_count = details.get("nInserted", 0)

            logger.info(f"[Info] Number of inserts: {inserted_count} ")
            for err in write_errors:
                logger.info(f"[Error Insert] Index {err['index']}: {err['errmsg']}")

def main():
    MONGODB_URI = os.getenv('MONGODB_URI')
    ## Connect to MongoDB DB:
    client = MongoClient(MONGODB_URI)
    db = client['fin-statements']
    logger.info('[Info] Connected to MongoDB.')
    new_collection = db['new']

    ## Initialize S3 client
    s3_client = get_s3_client()
    bucket_name = os.getenv('BUCKET_NAME')
    
    # files = list_files(s3_client, bucket_name)
    # if files:
    #     print(files)
    for date in [today, yesterday]:
        path_download_excel = path_download + '/excel/'
        path_download_html = path_download + '/html/'
        os.makedirs(path_download_html, exist_ok=True)
        os.makedirs(path_download_excel, exist_ok=True)
        update(date=date, path_download_excel=path_download_excel, path_download_html=path_download_html, path_export=path_export, collection=new_collection)
    
    print('uploading files:')
    sync_directory_to_s3(
        local_directory="",
        bucket_name=bucket_name,
        s3_client=s3_client,
        s3_prefix="update_db",
        exclude_patterns=["*.txt", "*.py", "liara*", "*.ipynb", "__pycache__/*", ".dockerignore", ".git*", "cron*", "README.md"]
    )
    print('upload done.')

if __name__=='__main__':
    main()
