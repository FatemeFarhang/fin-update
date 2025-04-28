## Downloading the html files and in case of error, the excel files of the reports

import pandas as pd
import re
import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import logging
from multiprocessing import Pool
from datetime import datetime

logger = logging.getLogger('download')

headers_html = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Referer': 'https://codal.ir/ReportList.aspx?search&LetterType=-1&AuditorRef=-1&PageNumber=1&Audited&NotAudited&IsNotAudited=false&Childs&Mains&Publisher=false&CompanyState=-1&ReportingType=-1&Category=-1&CompanyType=1&Consolidatable&NotConsolidatable',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

headers_excel = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,fa;q=0.8',
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
}

se = requests.Session()
retries = Retry(total=6,
                backoff_factor=0.3,
                status_forcelist=[ 500, 502, 503, 504 ])
se.mount('https://', HTTPAdapter(max_retries=retries))

def download_excel(r, path_download_excel):
    try:
        response = se.get(
            url=r['excel_url'],
            headers= headers_excel,
            timeout=20
        )
        if response.status_code == 200:
            with open(path_download_excel+f'{r["trace_no"]}.xls', 'w', encoding='utf-8') as mainfile:
                mainfile.write(response.text)
        else:
            logger.info(f'Error |{r["trace_no"]}| Excel did not download')
    except Exception as ex:
        logger.info(f'Excel Connection Error |{r["trace_no"]}| {ex}')

def download_sheets(r, path_download_html, path_download_excel):
    if type(r['sheet_no'])==list:
        for sheet in r['sheet_no']:
            try:
                url, n_sub = re.subn(r'[sS]heetId=(\d+)', 'SheetId='+str(sheet), r['url'])
                response_sub = se.get(url= url, headers=headers_html) 
                if n_sub==0:
                    response_sub = se.get(url= r['url']+'&SheetId='+str(sheet), headers=headers_html)
                if response_sub.status_code==200:
                    with open(path_download_html+f'{r["trace_no"]}-{sheet}.html', 'w', encoding='utf-8') as subfile:
                        subfile.write(response_sub.text)
                else:
                    logger.info(f'Error |{r["trace_no"]}| Sub sheet did not download - {sheet}')
            except Exception as sub_e:
                logger.info(f'Error |{r["trace_no"]}| Failed to download sub sheet - {sheet} - {sub_e}')
    else:
        try:
            response = se.get(
                url=r['url'],
                headers=headers_html,
                timeout=10
            )
            if response.status_code == 200:
                bs = BeautifulSoup(response.text, features='html5lib')
                opt_tags = bs.find(name='select').findChildren('option', recursive=False)
                selected = bs.find(name='select').findChildren('option', attrs={'selected': 'selected'}, recursive=False)
                opt_tags.remove(selected[0])
                selected_id = selected[0].attrs['value']
                # logger.info(selected_id)
                # if not Path(f'../download/fs-sheets/{r["trace_no"]}-{selected_id}.html').exists():
                with open(path_download_html+f'{r["trace_no"]}-{selected_id}.html', 'w', encoding='utf-8') as mainfile:
                    mainfile.write(response.text)

                opt_tags_value = [o.attrs['value'] for o in opt_tags]
                for sheet in opt_tags_value:
                    try:
                        url, n_sub = re.subn(r'[sS]heetId=(\d+)', 'SheetId='+str(sheet), r['url'])
                        response_sub = se.get(url= url, headers=headers_html)                    
                        if n_sub==0:
                            response_sub = se.get(url= r['url']+'&SheetId='+str(sheet), headers=headers_html)
                        if response_sub.status_code==200:
                            with open(path_download_html+f'{r["trace_no"]}-{sheet}.html', 'w', encoding='utf-8') as subfile:
                                subfile.write(response_sub.text)
                        else:
                            logger.info(f'Error |{r["trace_no"]}| Sub sheet did not download - {sheet}')
                            download_excel(r=r, path_download_excel=path_download_excel)
                    except Exception as sub_e:
                        logger.info(f'Error |{r["trace_no"]}| Failed to download sub sheet - {sheet} - {sub_e}')
                        download_excel(r=r, path_download_excel=path_download_excel)
            else:
                logger.info(f'Error |{r["trace_no"]}| Report html did not load')
                download_excel(r=r, path_download_excel=path_download_excel)
        except TypeError as tex:
            logger.info(f'Error |{r["trace_no"]}| TypeError in html - {tex}')
        except Exception as ex:
            logger.info(f'Error |{r["trace_no"]}| Failed to read report page html - {ex}')
            download_excel(r=r, path_download_excel=path_download_excel)