# -*- coding: UTF-8 -*-

from sqlalchemy import create_engine

from config import PROPERTIES


def gen_itretl_conn():
    # import mysql.connector
    # conn = mysql.connector.connect(
    #     host=PROPERTIES['HOSTNAME'],
    #     user=PROPERTIES['USERNAME'],
    #     password=PROPERTIES['PASSWORD'],
    #     db='itretl',
    # )
    engine_args = 'mysql://{user}:{password}@{host}/{db}?charset=utf8'.format(
        host=PROPERTIES['HOSTNAME'],
        user=PROPERTIES['USERNAME'],
        password=PROPERTIES['PASSWORD'],
        db='itretl',
    )
    engine = create_engine(engine_args)
    conn = engine.connect()
    return conn
