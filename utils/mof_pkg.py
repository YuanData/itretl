# -*- coding: UTF-8 -*-
import os
from pathlib import Path

import feather
import pandas as pd

from config import FEATHER_PATH, dstore_ip
from constant import *


class MOFData(object):

    def __init__(self, ex_or_im_port, currency, y_gen_start=None, y_gen_end=None, b_reload=False):
        self.ex_or_im_port = ex_or_im_port
        self.currency = currency
        self.mof_data_path = r'\\{dstore_ip}\dstore\Projects\data\mof-{EX_OR_IM_PORT}-{CURRENCY}'.format(
            dstore_ip=dstore_ip, EX_OR_IM_PORT=self.ex_or_im_port, CURRENCY=self.currency)
        self.y_gen_start = y_gen_start
        self.y_gen_end = y_gen_end
        self.b_reload = b_reload

        self.mof_tsv_path_lst = None
        self.mof_tsv_y_start = self.mof_tsv_m_start = None
        self.mof_tsv_y_end = self.mof_tsv_m_end = None

        self.country = None
        self.df_source = None

        self.gen_df_source()

    def get_tsv_first_latest(self):
        self.mof_tsv_path_lst = [os_path for os_path in Path(self.mof_data_path).glob('*.tsv')]
        mof_tsv_path_name_lst = [os_path.name for os_path in self.mof_tsv_path_lst]
        mof_tsv_path_name_lst.sort()
        first_mof_tsv, latest_mof_tsv = mof_tsv_path_name_lst[0], mof_tsv_path_name_lst[-1]
        print('NAS: [%s %s]' % (first_mof_tsv, latest_mof_tsv))
        # example: 2019-09.tsv
        self.mof_tsv_y_start, self.mof_tsv_m_start = int(first_mof_tsv[:4]), int(first_mof_tsv[5:7])
        self.mof_tsv_y_end, self.mof_tsv_m_end = int(latest_mof_tsv[:4]), int(latest_mof_tsv[5:7])

    def gen_df_source_from_nas(self, y_gen_start, y_gen_end):
        y_gen_start = self.mof_tsv_y_start if y_gen_start is None else y_gen_start
        y_gen_end = self.mof_tsv_y_end if y_gen_end is None else y_gen_end
        print('NAS: [%s %s]' % (y_gen_start, y_gen_end))
        chosen_path_lst = []
        for y in range(y_gen_start, y_gen_end + 1):
            chosen_path_lst.extend([os_path for os_path in self.mof_tsv_path_lst if str(y) in os_path.name])

        self.df_source = pd.DataFrame()
        for file in chosen_path_lst:
            print(file.name)
            df = pd.read_csv(file, encoding='utf-8', sep='\t',
                             usecols=['貨品分類', '中文貨名', '國家', '價值'])
            df.columns = [HSCODE, HSCODE_ZH, COUNTRY, VALUE]
            df['year'] = file.name[:4]
            df['month'] = file.name[5:7]
            df[HSCODE] = df[HSCODE].apply(lambda x: '0' + str(x) if len(str(x)) == 10 else str(x))

            self.df_source = pd.concat([self.df_source, df])

    def is_latest_feather(self, feather_path_lst):
        if feather_path_lst:
            last_feather_name = feather_path_lst[-1].name
            ck_y, ck_m = int(last_feather_name[-14:-10]), int(last_feather_name[-10:-8])
            b_latest_feather = self.mof_tsv_y_end == ck_y and self.mof_tsv_m_end == ck_m
            return b_latest_feather
        else:
            return False

    def gen_df_source(self):
        self.get_tsv_first_latest()

        self.y_gen_start = self.mof_tsv_y_start if self.y_gen_start is None else self.y_gen_start
        self.y_gen_end = self.mof_tsv_y_end if self.y_gen_end is None else self.y_gen_end

        feather_path_lst = [os_path for os_path in Path(FEATHER_PATH).glob('df_mof_export_usd_*.feather')]
        if self.is_latest_feather(feather_path_lst):
            self.df_source = feather.read_dataframe(feather_path_lst[-1])
            print('df from %s' % feather_path_lst[-1].name)
        else:
            self.gen_df_source_from_nas(self.y_gen_start, self.y_gen_end)
            str_df_mof_feather = \
                'df_mof_{EX_OR_IM_PORT}_{CURRENCY}_{START_Y}{START_M:02d}_{END_Y}{END_M:02d}.feather'.format(
                    EX_OR_IM_PORT=self.ex_or_im_port, CURRENCY=self.currency,
                    START_Y=self.y_gen_start, START_M=self.mof_tsv_m_start,
                    END_Y=self.y_gen_end, END_M=self.mof_tsv_m_end)
            df_mof_feather_path = os.path.join(FEATHER_PATH, str_df_mof_feather)
            feather.write_dataframe(self.df_source, df_mof_feather_path)
            print('df from NAS')


class FilterMOFData(object):
    def __init__(self, df_input):
        self.df_input = df_input.copy()
        self.df_output = None

    def filter_country(self, country):
        df = self.df_input.copy() if self.df_output is None else self.df_output.copy()
        self.df_output = df[df['國家'] == country]

    def filter_time(self, year_start, year_end, month_end, month_start=1):
        df = self.df_input.copy() if self.df_output is None else self.df_output.copy()
        df['int_year'] = df['year'].astype(int)
        df['int_month'] = df['month'].astype(int)
        df = df[df.eval('int_year >= {y_start} & int_year <= {y_end}'
                        ' & '
                        'int_month >= {m_start} & int_month <= {m_end}'.format(y_start=year_start, y_end=year_end,
                                                                               m_start=month_start, m_end=month_end))]
        df = df.copy()
        df.drop(columns=['int_year', 'int_month'], inplace=True)
        self.df_output = df
