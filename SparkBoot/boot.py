#!/usr/bin/python3
# -*- coding: utf-8 -*-
import pandas as pd
from pyspark.sql.functions import upper, pandas_udf, expr, col, count, lit, max
from pyspark.sql import SparkSession, Row, Column, Observation
from datetime import datetime, date
import sqlite3
import pyspark.pandas as ps
from pyutilb import YamlBoot
from pyutilb.cmd import *
from pyutilb.file import *
from pyutilb.log import log


# spark操作的基于yaml的启动器
class Boot(YamlBoot):

    def __init__(self):
        super().__init__()
        # 动作映射函数
        actions = {
            'read_csv': self.read_csv,
        }
        self.add_actions(actions)

        self.spark = SparkSession.builder.getOrCreate()

    # --------- 动作处理的函数 --------
    # 读csv数据
    def read_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_read('csv', config, default_options)

    # 读json数据
    def read_json(self, config):
        default_options = {
            'header': True
        }
        self.do_read('json', config, default_options)

    # 读jdbc数据
    def read_jdbc(self, config):
        default_options = {
            'header': True
        }
        self.do_read('jdbc', config, default_options)

    # 执行读数据
    def do_read(self, type, config, default_options):
        for table, option in config:
            option = {**default_options, **option}
            # 加载数据到df
            #df = self.spark.read.csv(**option)
            df = self.spark.read[type](**option)
            # 转table
            df.createOrReplaceTempView(table)

    # 执行sql
    def query_sql(self, config):
        for table, sql in config:
            df = self.spark.sql(sql)
            df.createOrReplaceTempView(table)


# cli入口
def main():
    # 基于yaml的执行器
    boot = Boot()
    # 读元数据：author/version/description
    dir = os.path.dirname(__file__)
    meta = read_init_file_meta(dir + os.sep + '__init__.py')
    # 步骤配置的yaml
    step_files, option = parse_cmd('SparkBoot', meta['version'])
    if len(step_files) == 0:
        raise Exception("Miss step config file or directory")
    try:
        # 执行yaml配置的步骤
        boot.run(step_files)
    except Exception as ex:
        log.error(f"Exception occurs: current step file is %s", boot.step_file, exc_info = ex)
        raise ex

if __name__ == '__main__':
    main()