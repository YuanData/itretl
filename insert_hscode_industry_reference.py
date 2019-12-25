# -*- coding: utf-8 -*-

import pandas as pd

from config import dstore_ip
from utils.db_tools import *


def gen_df_hscode_industry_reference():
    iy_hs_mapping_str = r'//{dstore_ip}/dstore/重要資料/產業hscode對照表.xlsx'
    iy_hs_mapping_xlsx = iy_hs_mapping_str.format(dstore_ip=dstore_ip)
    df_all_iy_xlsx = pd.read_excel(iy_hs_mapping_xlsx, 'sheet1')
    df_all_iy_xlsx.rename(columns={
        '編號': 'id',
        '選擇方式': 'type',
        '大項': 'major',
        '細項': 'minor',
        '6碼': 'hs6',
        '11碼': 'hs11',
        'HSCODE碼數': 'hs_digits',
        'hscode': 'hscode',
        'industry': 'industry',
        'reports_version_1': 'reports_version_1',
        'reports_version_1_order': 'reports_version_1_order',
        'reports_version_2': 'reports_version_2',
        'reports_version_2_order': 'reports_version_2_order',
        'reports_version_industry21': 'reports_version_industry21',
        'reports_version_industry21_order': 'reports_version_industry21_order',
    }, inplace=True)
    df_all_iy_xlsx = df_all_iy_xlsx.where((pd.notnull(df_all_iy_xlsx)), None)
    return df_all_iy_xlsx


def insert_hscode_industry_reference_process():
    df = gen_df_hscode_industry_reference()
    itrconversion_conn = gen_itr_conn(db='itrconversion')
    itrconversion_cur = itrconversion_conn.cursor()

    col_lst = df.columns.tolist()
    cols = "`,`".join([str(i) for i in col_lst])
    sql_cmd = "INSERT INTO `hscode_industry_reference` (`" + cols + "`) VALUES (" + "%s," * (len(col_lst) - 1) + "%s)"

    for _, row in df.iterrows():
        itrconversion_cur.execute(sql_cmd, tuple(row))
    itrconversion_conn.commit()

    itrconversion_cur.close()
    itrconversion_conn.close()


if __name__ == '__main__':
    insert_hscode_industry_reference_process()
