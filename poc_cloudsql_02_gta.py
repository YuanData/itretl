# -*- coding: UTF-8 -*-
import feather
import numpy as np
from pandas.core.dtypes.missing import isna

from utils.db_tools import gen_itr_conn

"""
CREATE TABLE `t02_gta_data_iran` (
    `year` INT(11) NOT NULL,
    `trade` VARCHAR(8) DEFAULT NULL,
    `hscode` CHAR(6) DEFAULT NULL,
    `reporter` VARCHAR(45) DEFAULT NULL,
    `partner` VARCHAR(45) DEFAULT NULL,
    `weight` BIGINT(20) NOT NULL,
    `value` BIGINT(20) NOT NULL,
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4;
"""

path = r'feather\02_GTA_data_IRAN_20191129.feather'


def insert_t02_gta_data_iran_ori():
    df = feather.read_dataframe(path)
    itrpoc_conn = gen_itr_conn(db='itrpoc')
    itrpoc_cur = itrpoc_conn.cursor()

    col_lst = df.columns.tolist()
    cols = "`,`".join([str(i) for i in col_lst])
    sql_cmd = "INSERT INTO `t02_gta_data_iran` (`" + cols + "`) VALUES (" + "%s," * (len(col_lst) - 1) + "%s)"

    for i, row in df.iterrows():
        itrpoc_cur.execute(sql_cmd, tuple(row))
        print(i)
    itrpoc_conn.commit()

    itrpoc_cur.close()
    itrpoc_conn.close()


class DataFrameInsert:

    def __init__(self, df, conn):
        self.df = df
        self.conn = conn
        self.cur = self.conn.cursor()

    def insert_statement(self, table_name):
        names = list(map(str, self.df.columns))
        wld = "%s"  # wildcard char
        bracketed_names = [column for column in names]
        col_names = ",".join(bracketed_names)
        wildcards = ",".join([wld] * len(names))
        str_insert_statement = "INSERT INTO {table} ({columns}) VALUES ({wld})".format(
            table=table_name, columns=col_names, wld=wildcards
        )
        return str_insert_statement

    def insert_data(self):
        temp = self.df
        column_names = list(map(str, temp.columns))
        ncols = len(column_names)
        data_list = [None] * ncols
        blocks = temp._data.blocks

        for b in blocks:
            if b.is_datetime:
                # return datetime.datetime objects
                if b.is_datetimetz:
                    # GH 9086: Ensure we return datetimes with timezone info
                    # Need to return 2-D data; DatetimeIndex is 1D
                    d = b.values.to_pydatetime()
                    d = np.atleast_2d(d)
                else:
                    # convert to microsecond resolution for datetime.datetime
                    d = b.values.astype("M8[us]").astype(object)
            else:
                d = np.array(b.get_values(), dtype=object)

            # replace NaN with None
            if b._can_hold_na:
                mask = isna(d)
                d[mask] = None

            for col_loc, col in zip(b.mgr_locs, d):
                data_list[col_loc] = col

        return column_names, data_list

    def df_chunk_insert(self, chunksize=None):
        keys, data_list = self.insert_data()
        nrows = len(self.df)

        if chunksize is None:
            chunksize = nrows

        chunks = int(nrows / chunksize) + 1

        for i in range(chunks):
            start_i = i * chunksize
            end_i = min((i + 1) * chunksize, nrows)
            print(start_i)
            if start_i >= end_i:
                print(end_i)
                break
            chunk_iter = zip(*[arr[start_i:end_i] for arr in data_list])
            self.cur.executemany(self.insert_statement('t02_gta_data_iran'), list(chunk_iter))
        self.conn.commit()


def insert_t02_gta_data_iran():
    df = feather.read_dataframe(path)
    itrpoc_conn = gen_itr_conn(db='itrpoc')

    agent = DataFrameInsert(df, itrpoc_conn)
    agent.df_chunk_insert(chunksize=10000)

    itrpoc_conn.close()


if __name__ == '__main__':
    insert_t02_gta_data_iran()
