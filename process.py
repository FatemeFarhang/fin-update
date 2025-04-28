import pandas as pd
import json
import numpy as np
import re
from glob import glob
import logging
from bs4 import BeautifulSoup
from xlcalculator import ModelCompiler
from xlcalculator import Evaluator
from multiprocessing import Pool
import jalali_pandas
from unidecode import unidecode
import string
from io import StringIO
from datetime import datetime
import os
from pathlib import Path

import warnings
## Filter FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

logger = logging.getLogger('process')

## Defining Custom Error Classes
class NoHeader(Exception):
    pass

class NoSharh(ValueError):
    pass

## Defining Custom JSON Encoder to Convert numpy objects
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        dtypes = (np.datetime64, np.complexfloating, pd._libs.tslibs.timestamps.Timestamp)
        if isinstance(obj, dtypes):
            return str(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            if any([np.issubdtype(obj.dtype, i) for i in dtypes]):
                return obj.astype(str).tolist()
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

## Defining Processing Chunks for Efficient Processing
def divide_chunks(l, n):
    for i in range(0, len(l), n): 
        yield l[i:i + n]

## Formatting as list of key values in JSON
def json_formatter(row):
    return dict(key=row.name, value=row.values.tolist())

## Processing cell values before export
def process_negative_values(s):
    return re.sub(r'\((\d+)\)', '-\\1', s)

def pre_text(text):
    if '%' in str(text):
        return str(text).replace('%', '').strip()
    elif str(text)=='nan':
        return text
    elif bool(re.match(r'^[+-]?([0-9]*[.])?[0-9]+$', str(text))):
       return str(text).replace(',', '').strip()
    elif bool(re.search(r'\d', str(text))):
       return str(text).replace(',', '').strip()
    else:
       return str(text).strip().translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)) ).replace('  ', ' ')
    
def convert_unicode(s):
    res = []
    if str(s)=='nan':
        return s
    for word in str(s).split():
        if bool(re.search(r'\d', str(word))):
            res.append(unidecode(word))
        else:
            res.append(word)
    return ' '.join(res)

## Find the datasource script tag in the HTML
def datasource_from_html(html_content):
    # Parse HTML content
    soup = BeautifulSoup(html_content, 'html5lib')
    # Find data
    table = soup.find_all('script', string=re.compile(pattern='datasource'))
    p = re.findall('var datasource = ({.*?});', str(table))
    if len(p)>0:
        data = json.loads(p[0])
        return data
    else:
        return None

## Find the option and table tags in the HTML
def read_html_table(html_content):
    bs = BeautifulSoup(html_content, features='html5lib')
    title = bs.find('option', attrs=dict(selected='selected'))
    return bs.select('table:not(.Hidden)'), title.attrs['value'], title.contents[0].replace('\n','').replace('\t', ''),

## Extract cells' formula from datasource and compute cells' data
def extract_from_data(df_con):
    df_con = df_con[['cellGroupName', 'columnSequence', 'rowSequence', 'value', 'address', 'cssClass', 'formula', 'isVisible', 'valueTypeName']]
    df_con = df_con.drop_duplicates().copy()
    df_con_dd = df_con[df_con['columnSequence'].isin(df_con.groupby('columnSequence')[['value', 'rowSequence']].apply(lambda x: x.values.tolist(), include_groups=False).drop_duplicates().index.values)].copy()
    df_con_dd = df_con_dd.groupby('columnSequence').apply(lambda x: x.drop_duplicates(['rowSequence'], keep='last'), include_groups=False).reset_index(level=[0], drop=False).reset_index(drop=True).copy()
    change = df_con_dd[df_con_dd['value'].str.contains('درصد|تغییر', na=False)&df_con_dd['cellGroupName'].eq('Header')]['columnSequence'].unique()
    df_con_dd = df_con_dd[~df_con_dd['columnSequence'].isin(change)]
    df_con_dd.loc[df_con_dd['valueTypeName'].eq('FormControl')&df_con_dd['value'].eq(''), 'value'] = 0
    df_con_dd['value'] = df_con_dd['value'].apply(pd.to_numeric, errors='ignore')
    df_con_dd['value_ex'] = df_con_dd['value']
    df_con_dd['formula'] = df_con_dd['formula'].astype(str)
    df_con_dd.loc[df_con_dd['formula'].astype(str).ne(''), 'formula'] = '=' + df_con_dd.loc[df_con_dd['formula'].astype(str).ne(''), 'formula'].str.replace('^=','', regex=True)
    df_con_dd.loc[df_con_dd['formula'].astype(str).ne(''), 'value_ex'] = df_con_dd.loc[df_con_dd['formula'].astype(str).ne(''), 'formula']

    compiler = ModelCompiler()
    my_model = compiler.read_and_parse_dict(df_con_dd.set_index('address')['value_ex'].to_dict())
    evaluator = Evaluator(my_model)

    for formula in my_model.formulae:
        try:
            val = evaluator.evaluate(formula)
            if type(val)!=float:
                if val.value=='#DIV/0!':
                    pass
                else:
                    df_con_dd.loc[df_con_dd['address'].eq(formula.replace('Sheet1!', '')), 'value'] = val.value
            else:
                df_con_dd.loc[df_con_dd['address'].eq(formula.replace('Sheet1!', '')), 'value'] = val
        except Exception:
            pass

    df_con_dd = df_con_dd[df_con_dd['isVisible'].eq(True)]

    return df_con_dd

## Construct table from the cells' data
def json_from_data(df_con_dd):
    cons = pd.DataFrame(columns=sorted(df_con_dd['columnSequence'].unique()))
    if not df_con_dd['cellGroupName'].eq('Header').any():
        raise NoHeader
    header= df_con_dd[df_con_dd['cellGroupName'].eq('Header')]['rowSequence'].unique()
    sharh = df_con_dd[df_con_dd['value'].eq('شرح')]['columnSequence'].values
    if len(sharh)==0:
        raise NoSharh
    rows = df_con_dd.groupby(['rowSequence'])
    for ind, group in rows:
        construct_row = group.T.set_axis(group['columnSequence'], axis='columns')
        cons.loc[ind] = construct_row.loc['value']

    cons = cons.set_axis(pd.MultiIndex.from_arrays(cons.loc[header].ffill().ffill(axis=1).values), axis='columns').iloc[max(header):]
    sharh_index = cons.columns.get_indexer_for(cons.filter(like='شرح').columns.unique())
    result = []
    for index_c in sorted(sharh_index, reverse=True):
        sliced_df = cons.iloc[:, index_c:]
        # sliced_df = sliced_df[~sliced_df.iloc[:, 1:].astype(str).map(len).eq(0).all(axis=1)]
        sliced_df = sliced_df[~sliced_df.iloc[:, 1:].isna().all(axis=1)]
        sliced_df = sliced_df.replace(to_replace=float('NaN'),value=None)
        fields_dict = dict(data=sliced_df.iloc[:, 1:].T.set_axis(sliced_df.iloc[:, 0], axis='columns').apply(json_formatter, axis=0).values.tolist())
        fields_dict['columns'] = sliced_df.columns[1:].to_list()
        result.append(fields_dict)
        cons = cons.iloc[:, :index_c]

    return result

## Construct table from HTML tag
def json_from_html_table(tables):
    header_table = [t for t in tables if t.select('th:not(.Hidden)')]
    header = []
    if len(header_table)==0:
        raise NoHeader
    
    for h in header_table[0].select('th:not(.Hidden)'):
        header.append(convert_unicode(" ".join(h.findAll(string=re.compile(r'\S'))).replace('  ', ' ')))
    if 'شرح' not in header:
        raise NoSharh
    
    data_table = [t for t in tables if t.select('td:not(.Hidden)')]
    data = data_table[0].select('tr:not(.Hidden, .HiddenRow)')
    all_data = []
    for row in data:
        cells = row.select('td:not(.Hidden)')
        row_data = [cell.text.strip() for cell in cells]
        all_data.append(row_data)
    df_table = pd.DataFrame(all_data, columns=header)
    df_table = df_table.map(convert_unicode).map(pre_text).map(process_negative_values)

    sharh = df_table.columns.get_indexer_for(['شرح'])
    result = []
    if -1 not in sharh:
        for index_c in sharh[::-1]:
            sliced_df = df_table.iloc[:, index_c:]
            sliced_df = sliced_df[~sliced_df.astype(str).map(len).eq(0).all(axis=1)]
            sliced_df = sliced_df[~sliced_df.eq('None').all(axis=1)]
            sliced_df = sliced_df.replace(to_replace=float('NaN'), value=None)
            fields_dict = dict(data=sliced_df.iloc[:, 1:].T.set_axis(sliced_df.iloc[:, 0], axis='columns').apply(json_formatter, axis=0).values.tolist())
            fields_dict['columns'] = sliced_df.columns[1:].to_list()
            result.append(fields_dict)
            df_table = df_table.iloc[:, :index_c]

    return result

## Extract data from xls files as HTML tags
def extract_tag_from_html(html_content):
    # Parse HTML content
    soup = BeautifulSoup(html_content, 'html5lib')
    div_tags = soup.find_all('h3')
        
    return div_tags

## Construct tables from exctracted data from Excels
def construct_from_html_tag(tag):
    if len(tag.parent.parent.select('table.rayanDynamicStatement')) > 0:
        tables = tag.parent.parent.select('table:not(.Hidden)')
        header_table = [t for t in tables if t.select('th:not(.Hidden)')]
        if len(header_table)==0:
            raise NoHeader
        df_table = pd.read_html(StringIO(str(tables)))[0]
        sharh = df_table.columns.get_indexer_for(df_table.filter(like='شرح').columns.unique())  

        if -1 in sharh:
            raise NoSharh
    else:
        if len(tag.parent.parent.select('table:not(.Hidden)')) > 0:
            tables = tag.parent.parent.select('table:not(.Hidden)')

            header_table = [t for t in tables if t.select('th:not(.Hidden)')]
            if len(header_table)==0:
                raise NoHeader
            header = []
            for h in header_table[0].select('th:not(.Hidden)'):
                header.append(convert_unicode(" ".join(h.find_all(string=re.compile(r'\S'))).replace('  ', ' ')))
            if 'شرح' not in header:
                raise NoSharh
            data_table = [t for t in tables if t.select('td:not(.Hidden)')]
            data = data_table[0].select('tr:not(.Hidden, .HiddenRow)')
            all_data = []
            for row in data:
                cells = row.select('td:not(.Hidden)')
                row_data = [cell.text.strip() for cell in cells]
                all_data.append(row_data)
            df_table = pd.DataFrame(all_data, columns=header)
        else:
            return []

    change = df_table.columns.get_indexer_for(df_table.filter(regex='درصد|تغییر').columns.unique())
    df_table = df_table.iloc[:, [j for j, c in enumerate(df_table.columns) if j not in change]].copy()
    df_table = df_table.map(convert_unicode).map(pre_text).map(process_negative_values)
    df_table = df_table.map(pd.to_numeric, errors='ignore')
    df_table = df_table.replace({'nan': None}).replace({'None': None})

    result = []
    sharh_index = df_table.columns.get_indexer_for(df_table.filter(like='شرح').columns.unique())

    for index_c in sorted(sharh_index, reverse=True):
        sliced_df = df_table.iloc[:, index_c:]
        sliced_df = sliced_df[~sliced_df.iloc[:, 1:].isna().all(axis=1)]
        sliced_df = sliced_df.replace(to_replace=float('NaN'), value=None)
        fields_dict = dict(data=sliced_df.iloc[:, 1:].T.set_axis(sliced_df.iloc[:, 0], axis='columns').apply(json_formatter, axis=0).values.tolist())
        fields_dict['columns'] = sliced_df.columns[1:].to_list()
        result.append(fields_dict)
        df_table = df_table.iloc[:, :index_c]

    return result

## Construct the final json file for import
def json_export(index_row, path_download_html, path_download_excel):
    dict_report = index_row.to_dict()
    dict_report['sheets'] = []
    sheets = [str(Path(p)) for p in glob(path_download_html+str(index_row['trace_no'])+'-*')]
    for fname in sheets:
        try:
            with open(fname, 'r', encoding='utf-8') as fbuffer:
                html_content = str(fbuffer.read())
        except Exception as exf:
            logger.info(f'[HTML File Error] | report no. [{index_row["trace_no"]} | {exf} | filename{fname}]')
        try:
            sheet_data = datasource_from_html(html_content=html_content)
            if sheet_data:
                dict_report.update({k: sheet_data.get(k, None) for k in ['title_Fa', 'title_En', 'period', 'yearEndToDate', 'kind', 'type', 'isAudited', 'state']})
                dict_report['version'] = re.findall(r'V(\d{1})', str(sheet_data['title_En']))
                dict_report['title_info'] = re.findall(r'(\b[^-]+\b)\s*\-', str(sheet_data['title_En']))
                if (str(sheet_data['title_En']).find('Other')>-1) or (str(sheet_data['title_En']).find('Child')>-1):
                    dict_report['to_insert'] = True
                    return dict_report
                elif len(sheet_data['sheets'])==0:
                    logger.info(f'[Empty DataSource] | report no. [{index_row["trace_no"]}] | filename {fname}')
                    continue
                else:
                    dict_sheet = dict(tables=[])
                    dict_sheet['sheet_id'] = sheet_data['sheets'][0]['code']
                    dict_sheet['title_Fa'] = sheet_data['sheets'][0]['title_Fa']
                    dict_sheet['title_En'] = sheet_data['sheets'][0]['title_En']
                    if (dict_sheet['title_En'].find('Interpretative')==-1): ### filtering this sheet to be added later
                        for table in sheet_data['sheets'][0]['tables']:
                            try:
                                if len(table['cells'])>0:
                                    extr_data = extract_from_data(pd.DataFrame(table['cells']))
                                    list_table = json_from_data(extr_data)
                                    for d in list_table:
                                        d['version_no'] = table.get('versionNo', None)
                                        d['title_En'] = table['title_En']
                                        d['title_Fa'] = table['title_Fa']
                                    dict_sheet['tables'].extend(list_table)
                            except NoSharh:
                                pass
                            except NoHeader:
                                pass
                            except Exception as e_build:
                                logger.info(f'[Error Datasource] | report no. [{index_row["trace_no"]}] | {e_build} | filename {fname}')
                        if len(dict_sheet['tables'])==0:
                            continue
                        else:
                            dict_report['sheets'].append(dict_sheet)
                    else:
                        dict_sheet['to_insert'] = True
                        dict_report['sheets'].append(dict_sheet)
            else:
                logger.info(f'[No Datasource] | report no. [{index_row["trace_no"]}] | filename {fname}')

                dict_sheet = dict(tables=[])
                try:
                    elements = read_html_table(html_content)
                    if (int(elements[1]) in [19, 30]):
                        continue #filtering these sheets
                    dict_sheet['title_Fa'] = elements[2]
                    dict_sheet['sheet_id'] = elements[1]
                    if len(elements[0]) > 0:
                        dict_sheet['tables'].extend(json_from_html_table(elements[0]))
                        dict_report['sheets'].append(dict_sheet)
                    else:
                        logger.info(f'[Empty HTML Table] | report no. [{index_row["trace_no"]}] | filename {fname}')
                except NoSharh:
                    pass
                except NoHeader:
                    pass
                except Exception as e_build:
                    logger.info(f'[Error HTML Table] | report no. [{index_row["trace_no"]}] | {e_build} | filename {fname}')
        except Exception as nfe:
            logger.info(f'[HTML Unknown Format] | report no. [{index_row["trace_no"]}] | {nfe} | filename {fname}')

    if os.path.exists(path_download_excel+str(index_row['trace_no'])+'.xls'):
        try:
            with open(path_download_excel+str(index_row['trace_no'])+'.xls', 'r', encoding='utf-8') as fbuffer:
                excel_content = str(fbuffer.read())
        except Exception as exf:
            logger.info(f'[ٍExcel File Error] | report no. [{index_row["trace_no"]}] | {exf}')
        try:
            tags = extract_tag_from_html(excel_content)
            for tag in tags:
                dict_sheet = dict(tables=[])
                title = tag.contents[0].replace('\n', '')
                try:
                    if title in [x['title_Fa'] for x in dict_report['sheets']]:
                        continue
                    if 'مدیره' in tag.contents[0]:
                        continue #filtering these sheets
                    if 'تفسیری' in tag.contents[0]:
                        dict_sheet['to_insert'] = True
                        dict_sheet['title_Fa'] = title
                        dict_report['sheets'].append(dict_sheet)
                        continue
                    tables = construct_from_html_tag(tag)
                    if len(tables)==0:
                        continue
                    else:
                        dict_sheet['tables'].extend(tables)
                        dict_sheet['title_Fa'] = title
                        dict_report['sheets'].append(dict_sheet)

                except NoSharh:
                    # logger.info(f'[Excel No Sharh] | report no. [{index_row["trace_no"]}]')
                    pass
                except NoHeader:
                    continue
                except AttributeError:
                    continue
                except Exception as e_build:
                    logger.info(f'[Excel Table Error] | report no. [{index_row["trace_no"]}] | {e_build}')

        except Exception as nfe:
            logger.info(f'[Excel Unknown Format] | report no. [{index_row["trace_no"]}] | {nfe}')
    if len(sheets)==0:
        dict_report['no_file'] = True

    return dict_report
