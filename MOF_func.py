# -*- coding: UTF-8 -*-

import re

import numpy as np
import pandas as pd

from config import dstore_ip
from constant import *


def gen_df_hs8_from_hs11_gpby(df_hs11):
    df_hs11['Hscode8'] = df_hs11[HSCODE].str[:8]
    df_hs11.drop(columns=[HSCODE, HSCODE_ZH], inplace=True)
    df_hs8 = df_hs11.groupby(['Hscode8', COUNTRY, 'year'])[VALUE].sum().reset_index()
    return df_hs8


def gen_df_all_iy_regex():
    iy_hs_mapping_str = r'//{dstore_ip}/dstore/重要資料/產業hscode對照表.xlsx'
    # iy_hs_mapping_str = r'//{dstore_ip}/dstore/重要資料/產業hscode對照表_20191015.xlsx'
    iy_hs_mapping_xlsx = iy_hs_mapping_str.format(dstore_ip=dstore_ip)
    df_all_iy_xlsx = pd.read_excel(iy_hs_mapping_xlsx, 'sheet1')
    # # df_all_iy_xlsx = pd.read_excel(r'SourceMaterial\產業hscode對照表.xlsx', 'sheet1')
    # # df_all_iy_xlsx = pd.read_excel(r'SourceMaterial\產業hscode對照表.xlsx', '重要資料版_節選16')

    # # 測試用
    # df_all_iy_xlsx = df_all_iy_xlsx[
    #     (df_all_iy_xlsx['大項'] == '16_機械及電機設備')
    #     | (df_all_iy_xlsx['大項'] == '全部產品')
    #     ]

    # 正式用
    df_all_iy_xlsx = df_all_iy_xlsx[
        (df_all_iy_xlsx['reports_version_1'] == 1)
        |
        (((df_all_iy_xlsx['大項'] == '遊艇') | (df_all_iy_xlsx['大項'] == '航太'))
         & (df_all_iy_xlsx['細項'] == '全部產品'))
        ]
    df_all_iy_xlsx = df_all_iy_xlsx[
        df_all_iy_xlsx['industry'] != '19_其他_傢俱'
        ]
    df_all_iy_xlsx = df_all_iy_xlsx[['選擇方式', 'industry', 'hscode']]

    df_all_iy_xlsx['industry'] = df_all_iy_xlsx['industry'].str.replace('_全部產品', '')

    df_all_iy_xlsx['industry'] = np.where(
        df_all_iy_xlsx['industry'] == '14_珠寶及貴金屬製品(71_天然珍珠或養珠, 寶石或次寶石, 貴金屬, 被覆貴金屬之金屬及其製品, 仿首飾, 鑄幣)',
        '14_珠寶及貴金屬製品',
        df_all_iy_xlsx['industry'])

    df_all_iy_xlsx['industry'] = np.where(
        df_all_iy_xlsx['選擇方式'] == '財政部定義產業',
        df_all_iy_xlsx['industry'].str.replace(re.compile(r'\d\d_'), ''),
        df_all_iy_xlsx['industry'])

    df_all_iy_regex = pd.DataFrame()
    for index, row in df_all_iy_xlsx.iterrows():
        lst_1 = row['hscode'].split(',')
        lst_2 = [s if len(s) <= 8 else s[:8] for s in lst_1]
        lst_3 = ['^' + s for s in lst_2]
        str_hs = '|'.join(lst_3)

        df = pd.DataFrame.from_dict({'選擇方式': row['選擇方式'],
                                     'industry': row['industry'],
                                     'hscode': str_hs, }, orient='index').T
        df_all_iy_regex = pd.concat([df_all_iy_regex, df])
    df_all_iy_regex.reset_index(drop=True, inplace=True)
    return df_all_iy_regex


def gen_df_all_iy_regex_reports_version_industry21():
    iy_hs_mapping_str = r'//{dstore_ip}/dstore/重要資料/產業hscode對照表.xlsx'
    iy_hs_mapping_xlsx = iy_hs_mapping_str.format(dstore_ip=dstore_ip)
    df_all_iy_xlsx = pd.read_excel(iy_hs_mapping_xlsx, 'sheet1')

    # 正式用
    df_all_iy_xlsx = df_all_iy_xlsx[
        (df_all_iy_xlsx['reports_version_industry21'] == 1)
    ]
    df_all_iy_xlsx.sort_values(['reports_version_industry21_order'], ascending=[True], inplace=True)
    df_all_iy_xlsx = df_all_iy_xlsx[['選擇方式', 'industry', 'hscode']]

    # df_all_iy_xlsx['industry'] = df_all_iy_xlsx['industry'].str.replace('_全部產品', '')

    df_all_iy_xlsx['industry'] = np.where(
        df_all_iy_xlsx['選擇方式'] == '財政部定義產業',
        df_all_iy_xlsx['industry'].str.replace(re.compile(r'\d\d_'), ''),
        df_all_iy_xlsx['industry'])

    df_all_iy_regex = pd.DataFrame()
    for index, row in df_all_iy_xlsx.iterrows():
        lst_1 = row['hscode'].split(',')
        lst_2 = [s if len(s) <= 8 else s[:8] for s in lst_1]
        lst_3 = ['^' + s for s in lst_2]
        str_hs = '|'.join(lst_3)

        df = pd.DataFrame.from_dict({'選擇方式': row['選擇方式'],
                                     'industry': row['industry'],
                                     'hscode': str_hs, }, orient='index').T
        df_all_iy_regex = pd.concat([df_all_iy_regex, df])
    df_all_iy_regex.reset_index(drop=True, inplace=True)
    return df_all_iy_regex


def rbind_df_by_iy_regex(df_input):
    df_all_iy_regex = gen_df_all_iy_regex()

    df_output = pd.DataFrame()
    for index, row in df_all_iy_regex.iterrows():
        df = df_input[df_input['Hscode8'].str.contains(row['hscode'], regex=True)].copy()
        df.insert(0, '選擇方式', row['選擇方式'])
        df.insert(1, 'Industry', row['industry'])
        print('[%s] %s' % (row['industry'], row['hscode']))
        df_output = pd.concat([df_output, df])
    return df_output
