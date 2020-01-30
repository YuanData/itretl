# -*- coding: UTF-8 -*-
import feather

from utils.db_tools import gen_itr_engine

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

path = r'data\feather\02_GTA_data_IRAN_20191129.feather'


def insert_t02_gta_data_iran():
    df = feather.read_dataframe(path)
    itrpoc_engine = gen_itr_engine(db='itrpoc')
    df.to_sql('t02_gta_data_iran', itrpoc_engine, if_exists="append", index=False, chunksize=10000)


if __name__ == '__main__':
    insert_t02_gta_data_iran()
