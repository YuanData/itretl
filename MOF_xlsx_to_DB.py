# -*- coding: utf-8 -*-
from pathlib import Path

import numpy as np
import pandas as pd

from config import HEATMAP_L_PATH, HS8_DIFF_RANK_PATH, REPORT_PATH
from utils.db_tools import *


# nspc_xlsx_to_db(period_str='2019年1-11月', period_type='ytd')
# nspc_xlsx_to_db(period_str='2019年11月', period_type='lsm')
# df.rename(columns={'產業': 'iy_type', '分類': 'iy_major', '國家': 'cy', '%s與前年同比_差額' % period_str: 'diff', }, inplace=True)
# df.to_sql('{str}_heatmap_{type}'.format(
#     str=period_str, type=period_type), itretl_conn, if_exists='replace', index=False)

def df_from_excel_replace_year_month(os_path):
    df = pd.read_excel(os_path)
    col_lst = df.columns
    col_rename_lst = [
        c.replace(
            '2019年1-11月', 'YTD-this-y').replace(
            '2019年11月', 'LSM-this-y').replace(
            '2018年1-11月', 'YTD-last-y').replace(
            '2018年11月', 'LSM-last-y')
        for c in col_lst]
    df.columns = col_rename_lst
    df = df.replace([np.inf, -np.inf], np.nan)
    return df


def tb_name_replace_year_month(os_path):
    tb_name = os_path.stem
    tb_name = tb_name.replace('2019年1-11月', 'YTD').replace('2019年11月', 'LSM')
    return tb_name


def nspc_xlsx_to_db(path):
    nspc_xlsx_lst = [os_path for os_path in Path(path).glob('新南向_*.xlsx')]
    for os_path in nspc_xlsx_lst:
        df = df_from_excel_replace_year_month(os_path)

        df['idx_type_iy_cy'] = df['產業'] + '_' + df['分類'] + '_' + df['國家']

        itretl_conn = gen_itretl_conn()
        tb_name = tb_name_replace_year_month(os_path)
        df.to_sql(tb_name, itretl_conn, if_exists='replace', index=False)
        itretl_conn.close()


def wld_xlsx_to_db_heatmap(path):
    wld_xlsx_lst = [os_path for os_path in Path(path).glob('全球_*.xlsx')]
    for os_path in wld_xlsx_lst:
        df = df_from_excel_replace_year_month(os_path)

        df['idx_type_iy'] = df['產業'] + '_' + df['分類']

        itretl_conn = gen_itretl_conn()
        tb_name = tb_name_replace_year_month(os_path)
        df.to_sql(tb_name, itretl_conn, if_exists='replace', index=False)
        itretl_conn.close()


def iy_xlsx_to_db(path, file_prefix):
    iy_xlsx_lst = [os_path for os_path in Path(path).glob('%s_*.xlsx' % file_prefix)]
    for os_path in iy_xlsx_lst:
        df = df_from_excel_replace_year_month(os_path)

        df['idx_type_iy'] = df['產業'] + '_' + df['分類']

        itretl_conn = gen_itretl_conn()
        tb_name = tb_name_replace_year_month(os_path)
        df.to_sql(tb_name, itretl_conn, if_exists='replace', index=False)
        itretl_conn.close()


def product_xlsx_to_db(path, file_prefix):
    product_xlsx_lst = [os_path for os_path in Path(path).glob('%s_*.xlsx' % file_prefix)]
    for os_path in product_xlsx_lst:
        df = df_from_excel_replace_year_month(os_path)

        itretl_conn = gen_itretl_conn()
        tb_name = tb_name_replace_year_month(os_path)
        df.to_sql(tb_name, itretl_conn, if_exists='replace', index=False)
        itretl_conn.close()


def iy_file_xlsx_to_db(path, file_name):
    nspc_xlsx_lst = [os_path for os_path in Path(path).glob(file_name)]
    for os_path in nspc_xlsx_lst:
        df = df_from_excel_replace_year_month(os_path)

        df['idx_type_iy'] = df['產業'] + '_' + df['分類']

        itretl_conn = gen_itretl_conn()
        tb_name = tb_name_replace_year_month(os_path)
        df.to_sql(tb_name, itretl_conn, if_exists='replace', index=False)
        itretl_conn.close()


if __name__ == '__main__':
    # nspc_xlsx_to_db(HEATMAP_L_PATH)
    # nspc_xlsx_to_db(HS8_DIFF_RANK_PATH)

    wld_xlsx_to_db_heatmap(HEATMAP_L_PATH)

    iy_xlsx_to_db(HS8_DIFF_RANK_PATH, '產業')
    product_xlsx_to_db(HS8_DIFF_RANK_PATH, '貨品')

    iy_file_xlsx_to_db(REPORT_PATH, '2019年1-11月_industry_hs8_diff_rank.xlsx')
