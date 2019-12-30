# -*- coding: UTF-8 -*-

from MOF_func import *
from config import *
from conversion import gen_country_area_raw
from utils.mof_pkg import MOFData, FilterMOFData


def rbind_df_by_area(df__iy_hs8_cy_yr):
    df_area, df_all_area_cy = gen_country_area_raw()
    lst_area = df_area['areaName'].to_list()
    del df_area
    df_concat = pd.DataFrame()
    for area in lst_area:
        if area == '全球':
            df_arcy = df__iy_hs8_cy_yr.copy()
            df_arcy['areaName'] = '全球'
        else:
            df_arcy = df_all_area_cy[df_all_area_cy['areaName'] == area].copy()
            df_arcy = pd.merge(df_arcy, df__iy_hs8_cy_yr, how='inner', left_on='countryName', right_on=COUNTRY)
        df_arcy = df_arcy.groupby(['選擇方式', 'Industry', 'areaName', 'year'])[VALUE].sum().reset_index()
        df_concat = pd.concat([df_concat, df_arcy])
    df_concat.rename(columns={'areaName': COUNTRY}, inplace=True)
    return df_concat


def gen_mof_export_value_with_gr_share_by_country(df__iy_hs8_cy_yr):
    dic_reports_version_1 = get_df_dic_reports_version_1()

    lst_iy_cy_yr = ['選擇方式', 'Industry', COUNTRY, 'year']
    df__iy_cy_yr = df__iy_hs8_cy_yr.groupby(lst_iy_cy_yr)[VALUE].sum().reset_index()

    df__iy_hs8_arcy_yr = rbind_df_by_area(df__iy_hs8_cy_yr)
    df__iy_cy_yr = pd.concat([df__iy_cy_yr, df__iy_hs8_arcy_yr])

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

    df_to_multiple_sheet(cy_lst, df__iy_cy_yr)


def df_to_multiple_sheet(cy_lst, df__iy_cy_yr):
    excel_file = os.path.join(export_value_with_gr_share_by_cy, 'export_value_with_gr_share_by_country.xlsx')
    writer = pd.ExcelWriter(excel_file, engine='xlsxwriter')
    for cy in cy_lst:
        df = df__iy_cy_yr[df__iy_cy_yr[COUNTRY] == cy]
        df = df[['reports_version_2_order', COUNTRY, 'Industry', 'year', 'Value']]
        df.set_index(['reports_version_2_order', COUNTRY, 'Industry', 'year'], inplace=True)
        df = df.unstack()
        df = df.fillna(0)
        df.columns = ['{Year}_{Value}'.format(Year=t[1], Value=t[0]) for t in tuple(df.columns)]
        df.reset_index(inplace=True)
        total_2019 = df[df['Industry'] == '總額']['2019_Value'].values[0]
        df['出口成長率(％)'] = (df['2019_Value'] - df['2018_Value']) / df['2018_Value']
        df['3年出口複合成長率(％)'] = np.power(df['2019_Value'] / df['2016_Value'], 1. / 3) - 1
        df['佔比(％)'] = df['2019_Value'] / total_2019

        df.to_excel(writer, sheet_name=cy, index=False)
        workbook, worksheet = writer.book, writer.sheets[cy]
        col_lst = list(df.columns)
        idx_lst_of_percent = [i for i, col_name in enumerate(col_lst) if '％' in col_name]
        idx_lst_of_amount = [i for i, col_name in enumerate(col_lst) if 'Value' in col_name]

        for idx in idx_lst_of_percent:
            worksheet.set_column(idx, idx, width=None, cell_format=workbook.add_format({'num_format': '0.00%'}))
        for idx in idx_lst_of_amount:
            worksheet.set_column(idx, idx, width=None, cell_format=workbook.add_format({'num_format': '#,##0'}))

    writer.save()
    writer.close()


if __name__ == '__main__':
    mof_data = MOFData('export', 'usd', y_gen_start=2016)
    year_start = 2016
    year_end = 2019

    mof_yt = FilterMOFData(mof_data.df_source)
    mof_yt.filter_time(year_start=year_start, year_end=year_end, month_end=11)
    df_yt__hs11_raw = mof_yt.df_output
    df_yt__hs8_raw = gen_df_hs8_from_hs11_gpby(df_yt__hs11_raw)
    df_yt__iy_hs8_cy_yr = rbind_df_by_iy_regex(df_yt__hs8_raw, 'reports_version_2')
    del mof_yt, df_yt__hs11_raw

    # mof_1m = FilterMOFData(mof_data.df_source)
    # mof_1m.filter_time(year_start=year_start, year_end=year_end, month_start=11, month_end=11)
    # df_1m__hs11_raw = mof_1m.df_output
    # df_1m__hs8_raw = gen_df_hs8_from_hs11_gpby(df_1m__hs11_raw)
    # df_1m__iy_hs8_cy_yr = rbind_df_by_iy_regex(df_1m__hs8_raw, 'reports_version_2')
    # del mof_1m, df_1m__hs11_raw

    del mof_data

    gen_mof_export_value_with_gr_share_by_country(df_yt__iy_hs8_cy_yr)
