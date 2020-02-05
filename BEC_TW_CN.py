# -*- coding: UTF-8 -*-

from pathlib import Path

import feather
import numpy as np
import pandas as pd

from utils.db_tools import gen_itr_conn


def gen_df_bec_hs():
    df_bec_raw_path_str = r'SourceMaterial\df_bec_raw.feather'
    df_bec_raw_path = Path(df_bec_raw_path_str)

    if df_bec_raw_path.exists():
        df_bec_raw = feather.read_dataframe(df_bec_raw_path)
    else:
        bec_xlsx = r'SourceMaterial\BEC轉碼表_對應.xlsx'
        xls = pd.ExcelFile(bec_xlsx)
        df_bec_raw = pd.read_excel(xls, '對應')
        df_bec_raw.rename(columns={'Unnamed: 4': 'hs6'}, inplace=True)

        feather.write_dataframe(df_bec_raw, df_bec_raw_path_str)
        print('df_bec_from_xlsx from BEC轉碼表_對應.xlsx')
    return df_bec_raw


def gen_df_bec_by_hs6():
    df_bec_by_hs6_path_str = r'SourceMaterial\df_bec_by_hs6.feather'
    df_bec_by_hs6_path = Path(df_bec_by_hs6_path_str)

    if df_bec_by_hs6_path.exists():
        df_bec_by_hs6 = feather.read_dataframe(df_bec_by_hs6_path)
    else:
        df_bec_raw = gen_df_bec_hs()
        df_bec_by_hs6 = pd.DataFrame()
        for index, row in df_bec_raw.iterrows():
            df = pd.DataFrame()
            df['hs6'] = pd.Series(row['hs6'].split(','))
            df['category'] = row['大類']
            df_bec_by_hs6 = pd.concat([df_bec_by_hs6, df])

        df_bec_by_hs6 = df_bec_by_hs6.drop_duplicates(['hs6'])
        feather.write_dataframe(df_bec_by_hs6, df_bec_by_hs6_path_str)
    return df_bec_by_hs6


def bec_tw_cn():
    sql_query = """
    SELECT 
        SUBSTRING(HSCode, 1, 6) HS6, year, SUM(ValueUSD) ValueUSD
    FROM
        itrade.mof_data
    WHERE
        trade_flow = 'export'
            AND Country = '中國大陸'
            AND year >= 2017
    GROUP BY SUBSTRING(HSCode, 1, 6) , year
    """
    itrade_conn = gen_itr_conn(db='itrade')
    df = pd.read_sql(sql_query, itrade_conn)
    df_bec_by_hs6 = gen_df_bec_by_hs6()
    df = df.set_index(['HS6', 'year']).unstack()
    df.columns = ['{year}'.format(year=t[1]) for t in tuple(df.columns)]
    df.reset_index(inplace=True)
    df = df.where((pd.notnull(df)), 0)

    df_result = pd.merge(df, df_bec_by_hs6, how='left', left_on='HS6', right_on='hs6')

    df_result['category'] = np.where(df_result['hs6'].isnull(), '其他', df_result['category'])
    df_result = df_result[['category', 'hs6',
                           # '2014', '2015', '2016',
                           '2017', '2018', '2019']]
    df_result.to_excel('MOF_TW_usd_export_to_CN__raw_data.xlsx', index=False)

    df_groupby_bec = df_result.groupby(['category'])[
        # '2014', '2015', '2016',
        '2017', '2018', '2019'].sum().reset_index()
    df_groupby_bec.to_excel('MOF_TW_usd_export_to_CN__BEC.xlsx', index=False)


if __name__ == '__main__':
    bec_tw_cn()
