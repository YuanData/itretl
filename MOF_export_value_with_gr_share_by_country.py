# -*- coding: UTF-8 -*-

from MOF_func import *
from config import *
from conversion import gen_df_hs_convert_zh
from utils.mof_pkg import MOFData, FilterMOFData

df_hs_convert_zh = gen_df_hs_convert_zh(hs_digital=8)  # 併HS中文貨名
RANK_TOP_NUM_5 = 5


def gen_mof_export_value_with_gr_share_by_country(df__iy_hs8_cy_yr):
    dic_reports_version_1 = get_df_dic_reports_version_1()

    lst_iy_cy_yr = ['選擇方式', 'Industry', COUNTRY, 'year']
    df__iy_cy_yr = df__iy_hs8_cy_yr.groupby(lst_iy_cy_yr)[VALUE].sum().reset_index()
    df__iy_cy_yr = pd.merge(df__iy_cy_yr, dic_reports_version_1, how='left',
                            left_on='Industry', right_on='reports_version_2_ind_name')
    df__iy_cy_yr.drop(columns=['reports_version_2_ind_name'], inplace=True)
    del dic_reports_version_1

    df_cy_order = df__iy_cy_yr[
        (df__iy_cy_yr['year'] == '2019')
        & (df__iy_cy_yr['Industry'] == '總額')
        ].copy()
    df_cy_order = df_cy_order.sort_values(['Value'], ascending=[False])
    cy_lst = df_cy_order[COUNTRY].tolist()
    cy_lst = cy_lst[:10]
    del df_cy_order

    excel_file = os.path.join(export_value_with_gr_share_by_cy, 'export_value_with_gr_share_by_country.xlsx')
    writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
    for cy in cy_lst:
        df = df__iy_cy_yr[df__iy_cy_yr[COUNTRY] == cy]
        df.to_excel(writer, sheet_name=cy, index=False)
    writer.save()


if __name__ == '__main__':
    mof_data = MOFData('export', 'usd', y_gen_start=2017)
    year_start = 2017
    year_end = 2019

    # mof_yt = FilterMOFData(mof_data.df_source)
    # mof_yt.filter_time(year_start=year_start, year_end=year_end, month_end=11)
    # df_yt__hs11_raw = mof_yt.df_output
    # df_yt__hs8_raw = gen_df_hs8_from_hs11_gpby(df_yt__hs11_raw)
    # df_yt__iy_hs8_cy_yr = rbind_df_by_iy_regex(df_yt__hs8_raw, 'reports_version_2')
    # del mof_yt, df_yt__hs11_raw

    mof_1m = FilterMOFData(mof_data.df_source)
    mof_1m.filter_time(year_start=year_start, year_end=year_end, month_start=11, month_end=11)
    df_1m__hs11_raw = mof_1m.df_output
    df_1m__hs8_raw = gen_df_hs8_from_hs11_gpby(df_1m__hs11_raw)
    df_1m__iy_hs8_cy_yr = rbind_df_by_iy_regex(df_1m__hs8_raw, 'reports_version_2')
    del mof_1m, df_1m__hs11_raw

    del mof_data

    gen_mof_export_value_with_gr_share_by_country(df_1m__iy_hs8_cy_yr)
