# -*- coding: UTF-8 -*-
import glob

import pandas as pd

from config import dstore_ip, ds_ip


def gen_df_hs_convert_zh(hs_digital):
    # full_hscode11 = r'\\{ds}\ds\01_jack\jack工作內容\03_查出口shiny\00_data_transfer\data\full_hscode11.xlsx'.format(
    #     ds=ds_ip)
    # xls = pd.ExcelFile(full_hscode11)
    # df_full_hscode11 = pd.read_excel(xls, 'full_hscode11')

    full_hscode11 = sorted(glob.glob(
        r'\\{dstore_ip}\dstore\Projects\data\hscode_data\full*.tsv'.format(dstore_ip=dstore_ip)))[-1]
    df_full_hscode11 = pd.read_csv(full_hscode11, sep='\t')

    hs_n = 'hs%s' % hs_digital
    hs_n_str = 'hs%sstr' % hs_digital
    df_full_hscode11[hs_n_str] = df_full_hscode11[hs_n].astype(str)
    df_full_hscode11[hs_n_str] = df_full_hscode11[hs_n_str].str.zfill(width=hs_digital)

    hs_n_cn = 'hs%scn' % hs_digital
    df_hs_mapping_zh = df_full_hscode11[[hs_n_str, hs_n_cn]]

    df_hs_mapping_zh = df_hs_mapping_zh.drop_duplicates([hs_n_str])

    df_hs_mapping_zh = df_hs_mapping_zh.set_index(hs_n_str)

    df_hs_mapping_zh.rename(columns={hs_n_cn: 'Hscode{}_Chinese'.format(hs_digital)}, inplace=True)

    return df_hs_mapping_zh


def gen_country_area(area_name):
    area_xlsx = r'\\{dstore_ip}\dstore\重要資料\area.xlsx'.format(dstore_ip=dstore_ip)
    # area_xlsx = r'\\{dstore_ip}\dstore\重要資料\area_20191120_v1.xlsx'.format(dstore_ip=dstore_ip)
    xls = pd.ExcelFile(area_xlsx)
    df_area = pd.read_excel(xls, '工作表1')

    df_concat = pd.DataFrame()
    for index, row in df_area.iterrows():
        df = pd.DataFrame()
        df['countryName'] = pd.Series(row['countryName'].split(','))
        df['areaName'] = row['areaName']
        df_concat = pd.concat([df_concat, df])
    df_concat = df_concat[df_concat['areaName'] == area_name]
    return df_concat


def gen_country_area_raw():
    area_xlsx = r'\\{dstore_ip}\dstore\重要資料\area.xlsx'.format(dstore_ip=dstore_ip)
    # area_xlsx = r'\\{dstore_ip}\dstore\重要資料\area_20191120_v1.xlsx'.format(dstore_ip=dstore_ip)
    xls = pd.ExcelFile(area_xlsx)
    df_area = pd.read_excel(xls, '工作表1')

    df_concat = pd.DataFrame()
    for index, row in df_area.iterrows():
        df = pd.DataFrame()
        df['countryName'] = pd.Series(row['countryName'].split(','))
        df['areaName'] = row['areaName']
        df_concat = pd.concat([df_concat, df])
    return df_area, df_concat
