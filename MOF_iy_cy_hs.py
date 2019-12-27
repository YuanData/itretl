# -*- coding: UTF-8 -*-

from MOF_func import *
from config import *
from conversion import gen_df_hs_convert_zh, gen_country_area_raw, gen_country_area
from utils.mof_pkg import MOFData, FilterMOFData

df_hs_convert_zh = gen_df_hs_convert_zh(hs_digital=8)  # 併HS中文貨名
RANK_TOP_NUM_5 = 5


def rank_hs8_diff(df, lst_gpby_cols, rank_top_num=None, diff_str='diff_%s'):
    df = df.sort_values(
        lst_gpby_cols + [diff_str % year_end],  # ['選擇方式', 'Industry', COUNTRY, 'diff_%s' % year_end]
        ascending=[True] * len(lst_gpby_cols) + [False],  # [True, True, True, False]
        na_position='last').reset_index(drop=True)
    df['Rank_NEG'] = df.groupby(lst_gpby_cols)[diff_str % year_end].rank(ascending=True, method='min')
    df['Rank_POS'] = df.groupby(lst_gpby_cols)[diff_str % year_end].rank(ascending=False, method='min')

    df['Rank_NEG'] = df['Rank_NEG'].astype(int)
    df['Rank_POS'] = df['Rank_POS'].astype(int)
    df['rank_type'] = np.where(df[diff_str % year_end] > 0, '增額', '減額')

    if rank_top_num is not None:
        df = df[((df['rank_type'] == '增額') & (df['Rank_POS'] <= rank_top_num)) |
                ((df['rank_type'] == '減額') & (df['Rank_NEG'] <= rank_top_num))]

    df['str_Rank_POS'] = '增額第' + df['Rank_POS'].apply(str)
    df['str_Rank_NEG'] = '減額第' + df['Rank_NEG'].apply(str)
    df['rank'] = np.where(df['rank_type'] == '增額', df['str_Rank_POS'], df['str_Rank_NEG'])
    df['rank_int'] = np.where(df['rank_type'] == '增額', df['Rank_POS'], df['Rank_NEG'])
    df.drop(columns=['Rank_NEG', 'Rank_POS', 'str_Rank_NEG', 'str_Rank_POS'], inplace=True)
    return df


def gen_dic_hiraky(lst_unst_yr_idxs, gpby_tar):
    lst_gpby_cols_yr = [c for c in lst_unst_yr_idxs if c != 'Hscode8']
    lst_gpby_cols = [c for c in lst_gpby_cols_yr if c != 'year']
    dic_hiraky = dict(
        lst_unst_yr_idxs=lst_unst_yr_idxs,  # eg. ['選擇方式', 'Industry', 'Hscode8', COUNTRY, 'year']
        lst_gpby_cols_yr=lst_gpby_cols_yr,  # eg. ['選擇方式', 'Industry', COUNTRY, 'year']
        lst_gpby_cols=lst_gpby_cols,  # eg. ['選擇方式', 'Industry', COUNTRY]
        gpby_tar=gpby_tar,  # eg. 'indst_of_cnty' or 'indst'
    )
    return dic_hiraky


def cal_share_per_yr(df, share_name, val='Value'):
    for y in range(year_start, year_end + 1):
        df['share_of_{share_name}_{y}'.format(share_name=share_name, y=y)] = \
            df['{Value}_{y}'.format(Value=val, y=y)] / df['sum_{share_name}_{y}'.format(share_name=share_name, y=y)]
    return df


def cal_growth_rate_per_yr(df, c_name='', val='Value'):
    last_y = None
    for y in range(year_start, year_end + 1):
        if last_y is None:
            last_y = y
        else:
            df['diff{c_name}_{y}'.format(c_name=c_name, y=y)] \
                = df['{Value}_{y}'.format(Value=val, y=y)] - df['{Value}_{y}'.format(Value=val, y=last_y)]
            df['growth_rate{c_name}_{y}'.format(c_name=c_name, y=y)] \
                = df['diff{c_name}_{y}'.format(c_name=c_name, y=y)] / df['{Value}_{y}'.format(Value=val, y=last_y)]
            df['growth_rate{c_name}_{y}'.format(c_name=c_name, y=y)] \
                = df['growth_rate{c_name}_{y}'.format(c_name=c_name, y=y)].replace([np.inf], np.nan)
            last_y += 1
    # df = df.fillna(0)
    return df


def gen_iy_hs8_process(df__iy_hs8_cy_yr):
    lst_iy_hs_yr = ['選擇方式', 'Industry', 'Hscode8', 'year']
    lst_iy_yr__rm_hs = ['選擇方式', 'Industry', 'year']
    lst_iy__rm_yr_hs = ['選擇方式', 'Industry']
    gpby_tar = 'indst'
    # 聚合 cy
    df__iy_hs8_yr = df__iy_hs8_cy_yr.groupby(lst_iy_hs_yr)[VALUE].sum().reset_index()

    # df_產業x年 = df_產業x貨品x年.gpby()
    df__iy_yr = df__iy_hs8_yr.groupby(lst_iy_yr__rm_hs)[VALUE].sum().reset_index()
    df__iy_yr.rename(columns={VALUE: 'sum_%s' % gpby_tar}, inplace=True)

    # df_產業= df_產業x年.UNSTACK(年)
    df_iy = df__iy_yr.set_index(lst_iy_yr__rm_hs).unstack()
    df_iy = df_iy.fillna(0)
    df_iy.columns = ['{sum_gpby_tar}_{y}'.format(sum_gpby_tar=t[0], y=t[1]) for t in tuple(df_iy.columns)]
    del df__iy_yr
    # df_產業x貨品 = df_產業x貨品x年.UNSTACK(年)
    df__iy_hs8 = df__iy_hs8_yr.set_index(lst_iy_hs_yr).unstack()
    df__iy_hs8 = df__iy_hs8.fillna(0)
    df__iy_hs8.columns = ['{Value}_{y}'.format(Value=t[0], y=t[1]) for t in tuple(df__iy_hs8.columns)]
    df__iy_hs8.reset_index(inplace=True)
    df__iy_hs8.set_index(lst_iy__rm_yr_hs, inplace=True)

    # df_產業x貨品.LEFT_JOIN(df_產業)
    df__iy_hs8 = pd.merge(df__iy_hs8, df_iy, how='left', left_index=True, right_index=True)
    df__iy_hs8.reset_index(inplace=True)

    df__iy_hs8_cal = cal_share_per_yr(df__iy_hs8, gpby_tar)
    df__iy_hs8_cal = cal_growth_rate_per_yr(df__iy_hs8_cal)
    del df__iy_hs8
    # [各產業] 貨品排名
    df__iy_hs8_rank = rank_hs8_diff(df__iy_hs8_cal, lst_iy__rm_yr_hs,
                                    rank_top_num=RANK_TOP_NUM_5
                                    )
    return df__iy_hs8_rank, df_iy


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #

def gen_hs8_rank_cy_diff_process(df__hs8_raw):
    lst_hs8_cy_yr = ['Hscode8', COUNTRY, 'year']
    lst_hs8_yr__rm_cy = ['Hscode8', 'year']
    lst_hs8__rm_yr_cy = ['Hscode8']
    gpby_tar = 'hs8gpby'
    df__hs8_cy_yr = df__hs8_raw.copy()

    # df_貨品x國= df_貨品x國x年.UNSTACK(年)
    df__hs8_cy = df__hs8_cy_yr.set_index(lst_hs8_cy_yr).unstack()
    df__hs8_cy = df__hs8_cy.fillna(0)
    df__hs8_cy.columns = ['{sum_gpby_tar}_{y}'.format(sum_gpby_tar=t[0], y=t[1]) for t in tuple(df__hs8_cy.columns)]
    df__hs8_cy.reset_index(inplace=True)
    df__hs8_cy.set_index(lst_hs8__rm_yr_cy, inplace=True)
    del df__hs8_cy_yr
    # df_貨品 = df_貨品x年.UNSTACK(年)
    df__hs8_yr = df__hs8_raw.groupby(lst_hs8_yr__rm_cy)[VALUE].sum().reset_index()
    del df__hs8_raw
    df__hs8_yr.rename(columns={VALUE: 'sum_%s' % gpby_tar}, inplace=True)
    df__hs8 = df__hs8_yr.set_index(lst_hs8_yr__rm_cy).unstack()
    df__hs8 = df__hs8.fillna(0)
    df__hs8.columns = ['{Value}_{y}'.format(Value=t[0], y=t[1]) for t in tuple(df__hs8.columns)]

    # df_產業x貨品.LEFT_JOIN(df_產業)
    df__hs8 = pd.merge(df__hs8, df__hs8_cy, how='left', left_index=True, right_index=True)
    df__hs8.reset_index(inplace=True)

    df__hs8_cy_cal = cal_share_per_yr(df__hs8, gpby_tar)
    df__hs8_cy_cal = cal_growth_rate_per_yr(df__hs8_cy_cal)
    del df__hs8, df__hs8_cy
    # [各貨品] 國排名
    df__hs8_cy_rank = rank_hs8_diff(df__hs8_cy_cal, lst_hs8__rm_yr_cy,
                                    rank_top_num=RANK_TOP_NUM_5
                                    )
    return df__hs8_cy_rank


def save_df__hs8_cy_rank_b4_unst(df, period_str, col_str):
    df = pd.merge(df_hs_convert_zh, df, how='right', left_index=True, right_on='Hscode8')
    df_col_tpl_lst = [
        ('Hscode8', 'HSCODE'),
        ('Hscode8_Chinese', '產品名'),
        ('Country', '國家'),
        ('Value_2018', '2018年%s月出口該國(千美元)' % col_str),
        ('Value_2019', '2019年%s月出口該國(千美元)' % col_str),
        ('diff_2019', '差額(千美元)'),
        ('growth_rate_2019', '成長率(％)'),
        ('share_of_hs8gpby_2019', '佔比 (出口該國佔該產品)'),
        ('rank_type', '增額或減額'),
        ('rank', '排名'),
        ('rank_int', '名次'),
    ]
    df_col_lst = [t[0] for t in df_col_tpl_lst]
    df_col_dic = {t[0]: t[1] for t in df_col_tpl_lst}
    df = df[df_col_lst].copy()
    df.rename(columns=df_col_dic, inplace=True)
    df__iy_cy_hs8_rank_xlsx = os.path.join(HS8_DIFF_RANK_PATH, '貨品_%s_hs8_cy_rank_L.xlsx' % period_str)
    df.to_excel(df__iy_cy_hs8_rank_xlsx, sheet_name='%s_hs8_cy_rank_L.xlsx' % period_str, index=False)


def gen_hs8_rank_cy_diff(df__hs8_raw, period_str, col_str):
    df__hs8_cy_rank = gen_hs8_rank_cy_diff_process(df__hs8_raw)
    df__hs8_cy_rank['share_of_diff_2019'] = df__hs8_cy_rank['diff_2019'] / (
            df__hs8_cy_rank['sum_hs8gpby_2019'] - df__hs8_cy_rank['sum_hs8gpby_2018'])

    df__hs8_cy_rank['市場'] = df__hs8_cy_rank[COUNTRY] + '\n( ' + df__hs8_cy_rank['diff_2019'].apply(  # 市差額
        lambda s: '{:,.0f}'.format(s)) + ' / ' + df__hs8_cy_rank['share_of_hs8gpby_2019'].apply(  # 佔比
        lambda s: '{:.4f}'.format(s)) + ' / ' + df__hs8_cy_rank['share_of_diff_2019'].apply(  # 市差額佔比
        lambda s: '{:.4f}'.format(s)) + ')'

    save_df__hs8_cy_rank_b4_unst(df__hs8_cy_rank, period_str, col_str)

    df__hs8_cy_rank = df__hs8_cy_rank[['Hscode8', 'rank', '市場']]
    df__hs8_cy_rank = df__hs8_cy_rank.drop_duplicates(['Hscode8', 'rank'])  # work around
    df__hs8_rank_cy_unst = df__hs8_cy_rank.set_index(['Hscode8', 'rank'])
    df__hs8_rank_cy_unst = df__hs8_rank_cy_unst.unstack()
    df__hs8_rank_cy_unst = df__hs8_rank_cy_unst.fillna(0)
    df__hs8_rank_cy_unst.columns = ['{rank}_{mkt}'.format(rank=t[1], mkt=t[0]) for t in
                                    tuple(df__hs8_rank_cy_unst.columns)]
    return df__hs8_rank_cy_unst


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #

def gen_iy_hs8_diff_rank(df__iy_hs8_cy_yr, period_str, col_str, df__hs8_rank_cy_unst):
    del df__hs8_rank_cy_unst
    df__iy_hs8_rank, df_iy = gen_iy_hs8_process(df__iy_hs8_cy_yr)
    df__iy_hs8_rank = pd.merge(df_hs_convert_zh, df__iy_hs8_rank, how='right', left_index=True, right_on='Hscode8')

    df__iy_hs8_rank_unst = unst_df__iy_hs8_rank(df__iy_hs8_rank, df_iy, period_str, col_str)

    df__iy_hs8_rank = df__iy_hs8_rank_reformat(df__iy_hs8_rank, col_str)  # 輸出格式調整

    # df__iy_hs8_rank = pd.merge(df__iy_hs8_rank, df__hs8_rank_cy_unst, how='left', left_on='HSCODE', right_index=True)

    df__iy_hs8_rank.reset_index(drop=True, inplace=True)
    industry_hs8_diff_rank_l_xlsx = os.path.join(HS8_DIFF_RANK_PATH, '產業_%s_iy_hs8_rank_L.xlsx' % period_str)
    df__iy_hs8_rank.to_excel(
        industry_hs8_diff_rank_l_xlsx, sheet_name='%s_iy_hs8_rank_L' % period_str, index=False)
    return df__iy_hs8_rank_unst


def unst_df__iy_hs8_rank(df__iy_hs8_rank, df_iy, period_str, col_str):
    df = df__iy_hs8_rank[
        ['選擇方式',
         'Industry',
         'Hscode8',
         'Hscode8_Chinese',
         'diff_2019',
         'growth_rate_2019',
         'share_of_indst_2019',
         'rank',
         ]].copy()
    df_iy['產業成長率'] = ((df_iy['sum_indst_2019'] - df_iy['sum_indst_2018']) / df_iy['sum_indst_2018'])
    # df_sum['產業成長率'] = ((df_sum['sum_indst_2019'] - df_sum['sum_indst_2018']) / df_sum['sum_indst_2018']) * 100
    # df_sum['產業成長率'] = df_sum['產業成長率'].apply(lambda s: '{:.2f}'.format(s))
    df_iy.drop(columns=['sum_indst_2017'], inplace=True)

    df_iy['產業差額'] = df_iy['sum_indst_2019'] - df_iy['sum_indst_2018']
    df_iy.rename(columns={
        'sum_indst_2018': '產業總額_2018年%s月' % col_str,
        'sum_indst_2019': '產業總額_2019年%s月' % col_str,
    }, inplace=True)

    df['growth_rate_2019'] = df['growth_rate_2019'] * 100
    df['share_of_indst_2019'] = df['share_of_indst_2019'] * 100
    df['growth_rate_2019'] = df['growth_rate_2019'].apply(lambda s: '{:.2f}'.format(s))
    df['growth_rate_2019'] = df['growth_rate_2019'].replace(['nan', 'inf'], '')
    df['share_of_indst_2019'] = df['share_of_indst_2019'].apply(lambda s: '{:.2f}'.format(s))

    df['產品'] = df['Hscode8'] + '\n' + df['Hscode8_Chinese'] + '\n( ' + df['diff_2019'].apply(
        lambda s: '{:,.0f}'.format(s)) + ' / ' + df['growth_rate_2019'].astype(str) + ')' \
        # + '\n[占比%]: ' + df['share_of_indst_2019'].astype(str)

    df.drop(columns=['Hscode8', 'Hscode8_Chinese', 'diff_2019', 'growth_rate_2019', 'share_of_indst_2019'],
            inplace=True)
    df = df.drop_duplicates(['選擇方式', 'Industry', 'rank'])  # 同排名處理 work around
    df = df.set_index(['選擇方式', 'Industry', 'rank']).unstack()
    df.columns = ['{l2}名{l1}'.format(l1=t[0], l2=t[1]) for t in tuple(df.columns)]

    df = pd.merge(df_iy, df, how='left', left_index=True, right_index=True)

    df.reset_index(inplace=True)
    df.rename(columns={'選擇方式': '產業', 'Industry': '分類', }, inplace=True)
    df.insert(3, '時間', '%s月' % col_str)
    industry_hs8_diff_rank_xlsx = os.path.join(REPORT_PATH, '%s_industry_hs8_diff_rank.xlsx' % period_str)
    df.to_excel(industry_hs8_diff_rank_xlsx, sheet_name='%s_iy_hs8_diff_rank' % period_str, index=False)
    del period_str
    return df


def df__iy_hs8_rank_reformat(df__iy_hs8_rank, col_str):
    df = df__iy_hs8_rank.copy()
    # 輸出格式調整 df__iy_hs8_rank [起]
    # df['share_of_indst_2018'] = df['share_of_indst_2018'] * 100
    # df['share_of_indst_2018'] = df['share_of_indst_2018'].apply(lambda s: '{:.2f}'.format(s))
    # df['share_of_indst_2019'] = df['share_of_indst_2019'] * 100
    # df['share_of_indst_2019'] = df['share_of_indst_2019'].apply(lambda s: '{:.2f}'.format(s))
    # df['share_of_indst_2018'] = df['share_of_indst_2018'].replace(['nan', 'inf'], '')
    # df['share_of_indst_2019'] = df['share_of_indst_2019'].replace(['nan', 'inf'], '')

    # df['growth_rate_2019'] = df['growth_rate_2019'] * 100
    # df['growth_rate_2019'] = df['growth_rate_2019'].apply(lambda s: '{:.2f}'.format(s))
    # df['growth_rate_2019'] = df['growth_rate_2019'].replace(['nan', 'inf'], '')

    df_col_tpl_lst = [
        ('選擇方式', '產業'),
        ('Industry', '分類'),
        ('sum_indst_2018', '產業總額_2018年%s月' % col_str),
        ('sum_indst_2019', '產業總額_2019年%s月' % col_str),
        ('Hscode8', 'HSCODE'),
        ('Hscode8_Chinese', '產品名'),
        ('Value_2018', '2018年%s月出口總額(千美元)' % col_str),
        ('Value_2019', '2019年%s月出口總額(千美元)' % col_str),
        # ('diff_2018', '2018年%s月產品總額與前年差額' % col_str),
        ('diff_2019', '差額(千美元)'),
        ('growth_rate_2019', '成長率(％)'),
        # ('share_of_indst_2018', '產品於產業佔比_2018年%s月' % col_str),
        ('share_of_indst_2019', '2019年%s月佔比 (產品佔該產業)' % col_str),
        ('rank_type', '增額或減額'),
        ('rank', '排名'),
        ('rank_int', '名次'),
    ]
    df_col_lst = [t[0] for t in df_col_tpl_lst]
    df_col_dic = {t[0]: t[1] for t in df_col_tpl_lst}
    df = df[df_col_lst]
    df.rename(columns=df_col_dic, inplace=True)
    # 輸出格式調整 df__iy_hs8_rank [訖]
    return df


def gen_excel_report_iy_hs8_rank(df_yt, df_1m):
    df = pd.concat([df_yt, df_1m], sort=False)
    df.sort_values(['產業', '分類', '時間'], ascending=[True, True, True], inplace=True)
    df = df.set_index(['產業', '分類', '時間'])
    industry_hs8_diff_rank_mix_time_xlsx = os.path.join(REPORT_PATH, '合併_industry_hs8_diff_rank.xlsx')
    df.to_excel(industry_hs8_diff_rank_mix_time_xlsx, sheet_name='合併_industry_hs8_diff_rank')


# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = #

def gen_iy_cy_hs8_process(df__iy_hs8_cy_yr):
    lst_iy_hs_cy_yr = ['選擇方式', 'Industry', 'Hscode8', COUNTRY, 'year']
    lst_iy_cy_yr__rm_hs = ['選擇方式', 'Industry', COUNTRY, 'year']
    lst_iy_cy__rm_hs_yr = ['選擇方式', 'Industry', COUNTRY]
    gpby_tar = 'indst_of_cnty'

    # df_產業x國x年 = df_產業x貨品x國x年.gpby()
    df__iy_cy_yr = df__iy_hs8_cy_yr.groupby(lst_iy_cy_yr__rm_hs)[VALUE].sum().reset_index()
    df__iy_cy_yr.rename(columns={VALUE: 'sum_%s' % gpby_tar}, inplace=True)

    # df_產業x國= df_產業x國x年.UNSTACK(年)
    df__iy_cy = df__iy_cy_yr.set_index(lst_iy_cy_yr__rm_hs).unstack()
    df__iy_cy = df__iy_cy.fillna(0)
    df__iy_cy.columns = ['{sum_gpby_tar}_{y}'.format(sum_gpby_tar=t[0], y=t[1]) for t in tuple(df__iy_cy.columns)]
    del df__iy_cy_yr
    # df_產業x貨品x國 = df_產業x貨品x國x年.UNSTACK(年)
    df__iy_hs8_cy = df__iy_hs8_cy_yr.set_index(lst_iy_hs_cy_yr).unstack()
    df__iy_hs8_cy = df__iy_hs8_cy.fillna(0)
    df__iy_hs8_cy.columns = ['{Value}_{y}'.format(Value=t[0], y=t[1]) for t in tuple(df__iy_hs8_cy.columns)]
    df__iy_hs8_cy.reset_index(inplace=True)
    df__iy_hs8_cy.set_index(lst_iy_cy__rm_hs_yr, inplace=True)

    # df_產業x貨品x國.LEFT_JOIN(df_產業x國)
    df__iy_hs8_cy = pd.merge(df__iy_hs8_cy, df__iy_cy, how='left', left_index=True, right_index=True)
    df__iy_hs8_cy.reset_index(inplace=True)

    df__iy_hs8_cy_cal = cal_share_per_yr(df__iy_hs8_cy, gpby_tar)
    df__iy_hs8_cy_cal = cal_growth_rate_per_yr(df__iy_hs8_cy_cal)
    # [各產業 x 各國] 貨品排名
    df__iy_cy_hs8_rank = rank_hs8_diff(df__iy_hs8_cy_cal, lst_iy_cy__rm_hs_yr,
                                       rank_top_num=RANK_TOP_NUM_5
                                       )
    return df__iy_cy_hs8_rank, df__iy_cy


def gen_iy_cy_hs8_diff_rank(df__iy_hs8_cy_yr, period_str, col_str, chosen_area=None):
    df__iy_cy_hs8_rank, df__iy_cy = gen_iy_cy_hs8_process(df__iy_hs8_cy_yr)

    # - - - - - - - - - - - 熱區圖 - - - - - - - - - - - #
    heatmap_process(df__iy_cy, period_str, chosen_area)  #
    # - - - - - - - - - - - - - - - - - - - - - - - - - #

    df__iy_cy_hs8_rank = pd.merge(df_hs_convert_zh, df__iy_cy_hs8_rank, how='right',
                                  left_index=True, right_on='Hscode8')

    # 選擇區域
    area_name = '全球' if chosen_area is None else chosen_area
    df_area = gen_country_area(area_name=area_name)
    df__iy_cy_hs8_rank = pd.merge(df_area, df__iy_cy_hs8_rank, how='inner', left_on='countryName', right_on='Country')
    df__iy_cy_hs8_rank.drop(columns=['countryName'], inplace=True)
    df__iy_cy_hs8_rank.reset_index(drop=True, inplace=True)

    # 輸出格式調整 df__iy_cy_hs8_rank [起]
    # df__iy_cy_hs8_rank['share_of_indst_of_cnty_2018'] = df__iy_cy_hs8_rank['share_of_indst_of_cnty_2018'] * 100
    # df__iy_cy_hs8_rank['share_of_indst_of_cnty_2018'] = df__iy_cy_hs8_rank['share_of_indst_of_cnty_2018'].apply(
    #     lambda s: '{:.2f}'.format(s))
    # df__iy_cy_hs8_rank['share_of_indst_of_cnty_2019'] = df__iy_cy_hs8_rank['share_of_indst_of_cnty_2019'] * 100
    # df__iy_cy_hs8_rank['share_of_indst_of_cnty_2019'] = df__iy_cy_hs8_rank['share_of_indst_of_cnty_2019'].apply(
    #     lambda s: '{:.2f}'.format(s))
    # df__iy_cy_hs8_rank['share_of_indst_of_cnty_2018'] = df__iy_cy_hs8_rank['share_of_indst_of_cnty_2018'].replace(
    #     ['nan', 'inf'], '')
    # df__iy_cy_hs8_rank['share_of_indst_of_cnty_2019'] = df__iy_cy_hs8_rank['share_of_indst_of_cnty_2019'].replace(
    #     ['nan', 'inf'], '')
    #
    # df__iy_cy_hs8_rank['growth_rate_2019'] = df__iy_cy_hs8_rank['growth_rate_2019'] * 100
    # df__iy_cy_hs8_rank['growth_rate_2019'] = df__iy_cy_hs8_rank['growth_rate_2019'].apply(
    #     lambda s: '{:.2f}'.format(s))
    # df__iy_cy_hs8_rank['growth_rate_2019'] = df__iy_cy_hs8_rank['growth_rate_2019'].replace(['nan', 'inf'], '')
    df_col_tpl_lst = [
        ('選擇方式', '產業'),
        ('Industry', '分類'),
        ('Country', '國家'),
        ('sum_indst_of_cnty_2018', '產業總額_2018年%s月' % col_str),
        ('sum_indst_of_cnty_2019', '產業總額_2019年%s月' % col_str),
        ('Hscode8', 'HSCODE'),
        ('Hscode8_Chinese', '產品名'),
        ('Value_2018', '2018年%s月出口總額(千美元)' % col_str),
        ('Value_2019', '2019年%s月出口總額(千美元)' % col_str),
        # ('share_of_indst_of_cnty_2018', '產品於該國家該產業總額佔比_2018年1-10月'),
        ('share_of_indst_of_cnty_2019', '佔比 (產品佔該國該產業)'),
        # ('diff_2018', '2018年1-10月產品該國家該產業總額與前年差額'),
        ('diff_2019', '差額(千美元)'),
        ('growth_rate_2019', '成長率(％)'),
        ('rank_type', '增額或減額'),
        ('rank', '排名'),
        ('rank_int', '名次'),
    ]
    df_col_lst = [t[0] for t in df_col_tpl_lst]
    df_col_dic = {t[0]: t[1] for t in df_col_tpl_lst}
    df__iy_cy_hs8_rank = df__iy_cy_hs8_rank[df_col_lst]
    df__iy_cy_hs8_rank.rename(columns=df_col_dic, inplace=True)
    # 輸出格式調整 df__iy_cy_hs8_rank [訖]
    file_prefix = '全球' if chosen_area is None else chosen_area
    df__iy_cy_hs8_rank_xlsx = os.path.join(HS8_DIFF_RANK_PATH,
                                           '%s_%s_iy_cy_hs8_rank_L.xlsx' % (file_prefix, period_str))
    df__iy_cy_hs8_rank.to_excel(df__iy_cy_hs8_rank_xlsx,
                                sheet_name='%s_%s_iy_cy_hs8_rank_L' % (file_prefix, period_str), index=False)


def heatmap_process(df__iy_cy, period_str, chosen_area):
    df__iy_cy.reset_index(inplace=True)
    df_area, df_country_area = gen_country_area_raw()

    df_area_sum = pd.DataFrame()
    for idx, row in df_area.iterrows():
        if row['areaName'] == '全球':
            df = df__iy_cy.copy()
        else:
            df_cy_area = df_country_area[df_country_area['areaName'] == row['areaName']]
            df = pd.merge(df_cy_area, df__iy_cy, how='inner', left_on='countryName', right_on='Country')
            df.drop(columns=['countryName', 'areaName', 'Country'], inplace=True)
        df_gpby = df.groupby(['選擇方式', 'Industry']).sum().reset_index()
        df_gpby.insert(2, 'Country', row['areaName'])
        df_area_sum = pd.concat([df_area_sum, df_gpby])
    df_area_sum.reset_index(drop=True, inplace=True)
    df_area_rank = df_area_sum[(df_area_sum['選擇方式'] == '全部產品') & (df_area_sum['Industry'] == '全部產品')].copy()
    df_area_rank = df_area_rank.sort_values(['sum_indst_of_cnty_2019'], ascending=[False]).reset_index(drop=True)
    area_rank_lst = df_area_rank['Country'].to_list()
    del df_area_rank

    df_cy_rank = df__iy_cy[(df__iy_cy['選擇方式'] == '全部產品') & (df__iy_cy['Industry'] == '全部產品')].copy()
    df_cy_rank = df_cy_rank.sort_values(['sum_indst_of_cnty_2019'], ascending=[False]).reset_index(drop=True)
    cy_rank_lst = df_cy_rank['Country'].to_list()
    del df_cy_rank

    df__iy_cy = pd.concat([df__iy_cy, df_area_sum])

    if chosen_area is not None:  # 僅選特定區域國家 [起]
        df_country_area = df_country_area[df_country_area['areaName'] == chosen_area]
        df__iy_cy = pd.merge(df_country_area, df__iy_cy, how='inner',
                             left_on='countryName', right_on='Country')
        df__iy_cy.drop(columns=['countryName', 'areaName'], inplace=True)
        df_area_sum = df_area_sum[
            (df_area_sum['Country'] == '全球') | (df_area_sum['Country'] == chosen_area)]
        df__iy_cy = pd.concat([df__iy_cy, df_area_sum])  # 僅選特定區域國家 [訖]

    df__all_product = df__iy_cy[df__iy_cy['Industry'] == '全部產品'].copy()
    df__all_product.sort_values(['sum_indst_of_cnty_2019'], ascending=[False], inplace=True)
    lst_cy_of_all_product = df__all_product['Country'].to_list()
    heatmap_cal_then_save(df__iy_cy, period_str, area_rank_lst, cy_rank_lst, lst_cy_of_all_product, chosen_area)


def cal_zs_by_div_std(df, col_str):
    iy_cy_std = df.groupby(['選擇方式', 'Industry'])[col_str].transform('std')
    df['iy_cy_zs'] = df[col_str] / iy_cy_std
    cy_iy_std = df.groupby(['選擇方式', 'Country'])[col_str].transform('std')
    df['cy_iy_zs'] = df[col_str] / cy_iy_std
    return df


def heatmap_cal_then_save(df__iy_cy, period_str, area_rank_lst, cy_rank_lst, lst_cy_of_all_product, chosen_area):
    df__iy_cy = cal_growth_rate_per_yr(df__iy_cy, c_name='_indst_of_cnty', val='sum_indst_of_cnty')
    df__iy_cy_diff = df__iy_cy[['選擇方式', 'Industry', COUNTRY, 'diff_indst_of_cnty_2019']].copy()
    df__iy_cy_diff = cal_zs_by_div_std(df__iy_cy_diff, 'diff_indst_of_cnty_2019')
    df__iy_cy_diff.rename(columns={'diff_indst_of_cnty_2019': '%s與前年同比_差額' % period_str}, inplace=True)

    df__iy_cy_growth_rate = df__iy_cy[
        ['選擇方式', 'Industry', COUNTRY, 'growth_rate_indst_of_cnty_2019']].copy()
    df__iy_cy_growth_rate = df__iy_cy_growth_rate.fillna(0)
    df__iy_cy_growth_rate = cal_zs_by_div_std(df__iy_cy_growth_rate, 'growth_rate_indst_of_cnty_2019')
    df__iy_cy_growth_rate['growth_rate_indst_of_cnty_2019'] = df__iy_cy_growth_rate[
        'growth_rate_indst_of_cnty_2019'].apply(lambda s: '{:.5f}'.format(s))
    df__iy_cy_growth_rate['growth_rate_indst_of_cnty_2019'] = df__iy_cy_growth_rate[
        'growth_rate_indst_of_cnty_2019'].replace(['nan', 'inf'], '')
    df__iy_cy_growth_rate.rename(
        columns={'growth_rate_indst_of_cnty_2019': '%s與前年同比_成長率' % period_str}, inplace=True)

    save_heatmap_to_xlsx(df__iy_cy_diff, period_str, 'diff', chosen_area)
    save_heatmap_to_xlsx(df__iy_cy_growth_rate, period_str, 'grat', chosen_area)
    save_unst_heatmap(df__iy_cy_diff, period_str, 'diff', area_rank_lst, cy_rank_lst, lst_cy_of_all_product,
                      chosen_area)
    save_unst_heatmap(df__iy_cy_growth_rate, period_str, 'grat', area_rank_lst, cy_rank_lst, lst_cy_of_all_product,
                      chosen_area)


def save_heatmap_to_xlsx(df__iy_cy_diff, period_str, heatmap_type, chosen_area):
    file_prefix = '全球' if chosen_area is None else chosen_area
    df__iy_cy_diff.rename(columns={'選擇方式': '產業', 'Industry': '分類', 'Country': '國家', }, inplace=True)
    df__iy_cy_diff_xlsx = os.path.join(HEATMAP_L_PATH,
                                       '%s_%s_heatmap_%s_L.xlsx' % (file_prefix, period_str, heatmap_type))
    df__iy_cy_diff.to_excel(df__iy_cy_diff_xlsx,
                            sheet_name='%s_%s_heatmap_%s_L' % (file_prefix, period_str, heatmap_type), index=False)


def save_unst_heatmap(df__iy_cy_diff, period_str, heatmap_type, area_rank_lst, cy_rank_lst, lst_cy_of_all_product,
                      chosen_area):
    df__iy_cy_diff.rename(columns={'選擇方式': '產業', 'Industry': '分類', 'Country': '國家', }, inplace=True)
    df__iy_cy_diff.drop(columns=['iy_cy_zs', 'cy_iy_zs'], inplace=True)
    df__iy_cy_diff = df__iy_cy_diff.set_index(['產業', '分類', '國家']).unstack()
    df__iy_cy_diff.reset_index(inplace=True)

    col_tpl = tuple(df__iy_cy_diff.columns)
    col_head = [t[0] for t in col_tpl if t[1] == '']
    col_original_order = [t[1] for t in col_tpl if t[1] != '']
    df__iy_cy_diff.columns = col_head + col_original_order
    area_rank_lst = [c for c in area_rank_lst if c in col_original_order]
    cy_rank_lst = [c for c in cy_rank_lst if c in col_original_order]
    cy_rank_lst = [c for c in cy_rank_lst if c in lst_cy_of_all_product]
    df__iy_cy_diff = df__iy_cy_diff[col_head + area_rank_lst + cy_rank_lst]
    df__iy_cy_diff['i_iy'] = 99
    iy_type_lst = ['全部產品', '財政部定義產業', '5+2產業', '財政部定義產業-54細項', '其他產業定義']
    for i, iy_type in enumerate(iy_type_lst):
        df__iy_cy_diff['i_iy'] = np.where(df__iy_cy_diff['產業'] == iy_type, i, df__iy_cy_diff['i_iy'])

    sort_key = '全球' if chosen_area is None else chosen_area
    df__iy_cy_diff.sort_values(['i_iy', sort_key], ascending=[True, False], inplace=True)
    df__iy_cy_diff.drop(columns=['i_iy'], inplace=True)
    df__iy_cy_diff_xlsx = os.path.join(HEATMAP_PATH, '%s_%s_heatmap_%s.xlsx' % (sort_key, period_str, heatmap_type))
    df__iy_cy_diff.to_excel(df__iy_cy_diff_xlsx, sheet_name='%s_heatmap_%s.xlsx' % (period_str, heatmap_type),
                            index=False)


if __name__ == '__main__':
    mof_data = MOFData('export', 'usd', y_gen_start=2017)
    year_start = 2017
    year_end = 2019

    mof_yt = FilterMOFData(mof_data.df_source)
    mof_yt.filter_time(year_start=year_start, year_end=year_end, month_end=11)

    mof_1m = FilterMOFData(mof_data.df_source)
    mof_1m.filter_time(year_start=year_start, year_end=year_end, month_start=11, month_end=11)
    del mof_data

    df_yt__hs11_raw = mof_yt.df_output
    df_1m__hs11_raw = mof_1m.df_output
    del mof_yt
    del mof_1m

    df_yt__hs8_raw = gen_df_hs8_from_hs11_gpby(df_yt__hs11_raw)
    df_yt__iy_hs8_cy_yr = rbind_df_by_iy_regex(df_yt__hs8_raw)
    del df_yt__hs11_raw

    df_1m__hs8_raw = gen_df_hs8_from_hs11_gpby(df_1m__hs11_raw)
    df_1m__iy_hs8_cy_yr = rbind_df_by_iy_regex(df_1m__hs8_raw)
    del df_1m__hs11_raw

    df_yt__hs8_rank_cy_unst = gen_hs8_rank_cy_diff(df_yt__hs8_raw, period_str='2019年1-11月', col_str='1-11')
    df_1m__hs8_rank_cy_unst = gen_hs8_rank_cy_diff(df_1m__hs8_raw, period_str='2019年11月', col_str='11')

    df_yt__iy_hs8_rank_unst = gen_iy_hs8_diff_rank(df_yt__iy_hs8_cy_yr, period_str='2019年1-11月', col_str='1-11',
                                                   df__hs8_rank_cy_unst=df_yt__hs8_rank_cy_unst)
    df_1m__iy_hs8_rank_unst = gen_iy_hs8_diff_rank(df_1m__iy_hs8_cy_yr, period_str='2019年11月', col_str='11',
                                                   df__hs8_rank_cy_unst=df_1m__hs8_rank_cy_unst)
    gen_excel_report_iy_hs8_rank(df_yt__iy_hs8_rank_unst, df_1m__iy_hs8_rank_unst)
    del df_yt__iy_hs8_rank_unst, df_1m__iy_hs8_rank_unst

    # choice_area = '新南向'
    choice_area = None
    gen_iy_cy_hs8_diff_rank(df_yt__iy_hs8_cy_yr, period_str='2019年1-11月', col_str='1-11', chosen_area=choice_area)
    gen_iy_cy_hs8_diff_rank(df_1m__iy_hs8_cy_yr, period_str='2019年11月', col_str='11', chosen_area=choice_area)
