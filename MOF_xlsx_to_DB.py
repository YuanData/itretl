# -*- coding: utf-8 -*-
from pathlib import Path

import numpy as np
import pandas as pd

from config import HEATMAP_L_PATH, HS8_DIFF_RANK_PATH, REPORT_PATH
from utils.db_tools import *

YTD_last_y = '2018年1-11月'
YTD_this_y = '2019年1-11月'
LSM_last_y = '2018年11月'
LSM_this_y = '2019年11月'


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
            YTD_this_y, 'YTD-this-y').replace(
            LSM_this_y, 'LSM-this-y').replace(
            YTD_last_y, 'YTD-last-y').replace(
            LSM_last_y, 'LSM-last-y')
        for c in col_lst]
    df.columns = col_rename_lst
    df = df.replace([np.inf, -np.inf], np.nan)
    return df


def tb_name_replace_year_month(os_path):
    tb_name = os_path.stem
    tb_name = tb_name.replace(YTD_this_y, 'YTD').replace(LSM_this_y, 'LSM')
    return tb_name


def df_idx_type_iy_cy(df):
    df['idx_type_iy_cy'] = df['產業'] + '_' + df['分類'] + '_' + df['國家']
    return df


def df_idx_type_iy(df):
    df['idx_type_iy'] = df['產業'] + '_' + df['分類']
    return df


def df_original(df):
    return df


def file_xlsx_to_db(path, file_name, df_func):
    file_xlsx_lst = [os_path for os_path in Path(path).glob(file_name)]
    for os_path in file_xlsx_lst:
        df = df_from_excel_replace_year_month(os_path)
        df = df_func(df)
        itretl_conn = gen_itretl_conn()
        tb_name = tb_name_replace_year_month(os_path)
        df.to_sql(tb_name, itretl_conn, if_exists='replace', index=False)
        itretl_conn.close()


if __name__ == '__main__':
    # file_xlsx_to_db(HEATMAP_L_PATH, '新南向_*.xlsx', df_idx_type_iy_cy)
    # file_xlsx_to_db(HS8_DIFF_RANK_PATH, '新南向_*.xlsx', df_idx_type_iy_cy)

    file_xlsx_to_db(HEATMAP_L_PATH, '全球_*.xlsx', df_idx_type_iy)

    file_xlsx_to_db(HS8_DIFF_RANK_PATH, '產業_*.xlsx', df_idx_type_iy)
    file_xlsx_to_db(HS8_DIFF_RANK_PATH, '貨品_*.xlsx', df_original)

    file_xlsx_to_db(REPORT_PATH, '2019年1-11月_industry_hs8_diff_rank.xlsx', df_idx_type_iy)
