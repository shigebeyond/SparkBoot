#!/usr/bin/python3
# -*- coding: utf-8 -*-
from shutil import copyfile
import pandas as pd
from pyspark.sql.functions import upper, pandas_udf, expr, col, count, lit, max
from pyspark.sql import SparkSession, Row, Column, Observation
from datetime import datetime, date
import pyspark.pandas as ps
from pyspark.sql.types import StringType
from pyutilb import YamlBoot, SparkDfProxy
from pyutilb.cmd import *
from pyutilb.file import *
from pyutilb.log import log
from pyutilb.util import set_var, get_var, replace_var

# spark操作的基于yaml的启动器
class Boot(YamlBoot):

    def __init__(self):
        super().__init__()
        # 动作映射函数
        actions = {
            'init_session': self.init_session,
            'cache': self.cache,
            'query_sql': self.query_sql,
            # 读动作
            'read_csv': self.read_csv,
            'read_json': self.read_json,
            'read_orc': self.read_orc,
            'read_parquet': self.read_parquet,
            'read_text': self.read_text,
            'read_jdbc': self.read_jdbc,
            # 写动作
            'write_csv': self.write_csv,
            'write_json': self.write_json,
            'write_orc': self.write_orc,
            'write_parquet': self.write_parquet,
            'write_text': self.write_text,
            'write_jdbc': self.write_jdbc,
        }
        self.add_actions(actions)
        # 不统计: 因yaml.dump()涉及到spark df就报错
        self.stat_dump = False
        # spark session
        self.spark = None
        # 要缓存df
        self.caching = False

    # 记录要注册的udf
    udfs = []

    # 记录要注册的udf，要延迟注册
    @classmethod
    def register_udf(cls, func, returnType = StringType()):
        cls.udfs.append((func, returnType))

    # --------- 动作处理的函数 --------
    # 初始化spark session
    def init_session(self, config):
        # 新建session
        app = config.get('app', 'SparkBoot')
        builder = SparkSession.builder.appName(app)
        if 'master' in config:
            builder.master(config['master'])
        self.spark = builder.enableHiveSupport().getOrCreate()
        # 注册udf
        for func, returnType in self.udfs:
            self.spark.udf.register(func.__name__, func, returnType=returnType)

    # 要缓存df
    def cache(self, steps):
        old = self.caching
        self.caching = True
        # 执行子步骤
        self.run_steps(steps)
        self.caching = old

    # --- 执行sql ---
    # 执行sql
    def query_sql(self, config):
        for table, sql in config.items():
            sql = replace_var(sql)
            # 查sql
            df = self.spark.sql(sql)
            # 加载df后的处理
            self.on_load_df(df, table)

    # 加载df后的处理
    def on_load_df(self, df, table):
        # 设为变量
        set_var(table, SparkDfProxy(df))
        # 转table
        df.createOrReplaceTempView(table)
        # 缓存
        if self.caching:
            df.cache()
        # show
        if self.debug:
            df.show()

    # --- 读数据 ---
    # 读csv数据
    def read_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_read('csv', config, default_options)

    # 读json数据
    def read_json(self, config):
        self.do_read('json', config)

    # 读orc数据
    def read_orc(self, config):
        self.do_read('orc', config)

    # 读parquet数据
    def read_parquet(self, config):
        self.do_read('parquet', config)

    # 读文本数据
    def read_text(self, config):
        self.do_read('text', config)

    # 读jdbc数据
    def read_jdbc(self, config):
        default_options = {
        }
        self.do_read('jdbc', config, default_options)

    # 执行读数据
    def do_read(self, type, config, default_options = None):
        for table, option in config.items():
            if isinstance(option, str): # 路径
                option = {'path': option}
            if default_options:
                option = {**default_options, **option}
            # 加载数据到df
            #df = self.spark.read.csv(**option)
            df = getattr(self.spark.read, type)(**option)
            # 加载df后的处理
            self.on_load_df(df, table)

    # --- 写数据 ---
    # 写csv数据
    def write_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_write('csv', config, default_options)

    # 写json数据
    def write_json(self, config):
        self.do_write('json', config)

    # 写orc数据
    def write_orc(self, config):
        self.do_write('orc', config)

    # 写parquet数据
    def write_parquet(self, config):
        self.do_write('parquet', config)

    # 写文本数据
    def write_text(self, config):
        self.do_write('text', config)

    # 写jdbc数据
    def write_jdbc(self, config):
        self.do_write('jdbc', config)

    # 执行写数据
    def do_write(self, type, config, default_options = None):
        for table, option in config.items():
            if isinstance(option, str): # 路径
                option = {'path': option}
            if default_options:
                option = {**default_options, **option}
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            # 将df输出到
            #df.write.csv(**option)
            getattr(df.write, type)(**option)

    # 生成要提交的作业文件+命令
    def generate_submiting_job(self, output, step_files, udf_file):
        if not os.path.exists(output):
            os.mkdir(output)
        # 生成入口文件bootmain.py

        # 复制udf文件

        # 复制步骤文件
        files = []
        for src in step_files:
            filename = os.path.basename(src)
            files.append(filename)
            copyfile(src, os.path.join(output, filename))

        # 生成命令
        cmd = f'''spark-submit bootmain.py \
    --master local \
    --driver-memory 2g \
    --executor-memory 2g \
    --files {','.join(files)}
        '''
        if udf_file is not None:
            cmd = f'''{cmd} \
    --py-files {udf_file}'''
        print("生成提交命令: " + cmd)
        write_file(os.path.join(output, 'submit.sh'), cmd)

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
    # 指定输出目录，则生成作业文件
    if option.output != None:
        boot.generate_submiting_job(option.output, step_files, option.udf)
        return
    try:
        # 执行yaml配置的步骤
        boot.run(step_files)
    except Exception as ex:
        log.error(f"Exception occurs: current step file is %s", boot.step_file, exc_info = ex)
        raise ex

if __name__ == '__main__':
    main()