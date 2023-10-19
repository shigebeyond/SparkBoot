#!/usr/bin/python3
# -*- coding: utf-8 -*-
import os
from shutil import copyfile
from pyspark.sql import SparkSession, Row, Column
from pyspark.sql.types import StringType
from pyspark.sql.functions import split, explode, length, udf
from pyutilb import YamlBoot, SparkDfProxy
from pyutilb.cmd import *
from pyutilb.file import *
from pyutilb.log import log
from pyutilb.util import *

# udf装饰器
def udf(func, returnType = StringType()):
    @wraps(func)
    def inner_wrapper(*args, **kwargs):
        func.__dict__['_returnType'] = returnType
        return func(*args, **kwargs)
    return inner_wrapper

# spark操作的基于yaml的启动器
class Boot(YamlBoot):

    def __init__(self):
        super().__init__()
        # 动作映射函数
        actions = {
            'init_session': self.init_session,
            'cache': self.cache,
            'persist': self.persist,
            'query_sql': self.query_sql,
            'drop_table': self.drop_table,
            'list_tables': self.list_tables,
            # 读批数据动作
            'read_csv': self.read_csv,
            'read_json': self.read_json,
            'read_orc': self.read_orc,
            'read_parquet': self.read_parquet,
            'read_text': self.read_text,
            'read_jdbc': self.read_jdbc,
            'read_table': self.read_table,
            # 读流数据动作
            'reads_csv': self.reads_csv,
            'reads_json': self.reads_json,
            'reads_orc': self.reads_orc,
            'reads_parquet': self.reads_parquet,
            'reads_text': self.reads_text,
            'reads_socket': self.reads_socket,
            'reads_kafka': self.reads_kafka,
            'reads_rate': self.reads_rate,
            # 写批数据动作
            'write_csv': self.write_csv,
            'write_json': self.write_json,
            'write_orc': self.write_orc,
            'write_parquet': self.write_parquet,
            'write_text': self.write_text,
            'write_jdbc': self.write_jdbc,
            'write_table': self.write_table,
            # 写流数据动作
            'writes_csv': self.writes_csv,
            'writes_json': self.writes_json,
            'writes_orc': self.writes_orc,
            'writes_parquet': self.writes_parquet,
            'writes_text': self.writes_text,
            'writes_console': self.writes_console,
            'writes_kafka': self.writes_kafka,
        }
        self.add_actions(actions)
        # 不统计: 因yaml.dump()涉及到spark df就报错
        self.stat_dump = False
        # spark session
        self.spark = None
        # 要缓存df
        self.caching = False
        self.persisting = False
        # 记录存储的表
        self.persist_tables = set()
        # 记录stream query
        self.squeries = []
        # 记录要注册的udf
        self.udfs = {}

    # 获得表对应的df
    def get_table_df(self, table):
        return self.spark.table(table)

    # --------- 动作处理的函数 --------
    # 初始化spark session
    @replace_var_on_params
    def init_session(self, config):
        # 读配置
        app = get_and_del_dict_item(config, 'app', 'SparkBoot')
        master = get_and_del_dict_item(config, 'master')
        log_level = get_and_del_dict_item(config, 'log_level', 'ERROR').upper()  # 日志级别,转大写
        # 新建session
        builder = SparkSession.builder.appName(app)
        if master is not None:
            builder.master(master)
        # 应用其他配置
        # builder.config(map = config) # pyspark3.4支持map参数, 3.2不支持
        for k,v in config.items():
            builder.config(k, v)
        self.spark = builder.enableHiveSupport().getOrCreate()
        self.spark.sparkContext.setLogLevel(log_level)
        # 注册udf
        for name, func in self.udfs.items():
            if not name.startswith('_'): # 非私有函数
                self.spark.udf.register(name, func) # 注册udf

    # 要缓存df
    def cache(self, steps):
        old = self.caching
        self.caching = True
        # 执行子步骤
        self.run_steps(steps)
        self.caching = old

    # 要存储df
    def persist(self, steps):
        old = self.persisting
        self.persisting = True
        # 执行子步骤
        self.run_steps(steps)
        self.persisting = old

    # 删除表
    def drop_table(self, table):
        self.spark.catalog.dropTempView(table)

    # 列出表
    def list_tables(self, _):
        log.debug(self.spark.catalog.listTables())

    # --- 执行sql ---
    # 执行sql
    @replace_var_on_params
    def query_sql(self, config):
        for table, sql in config.items():
            # 查sql
            log.debug(f"查sql并创建表[{table}]: {sql}")
            df = self.spark.sql(sql)
            # 加载df后的处理
            self.on_load_df(df, table, sql)

    def on_load_df(self, df, table, sql = None):
        '''
        加载df后的处理
        :param df 结果df，如果为空则表示表已存在，否则将df存为临时表
        :param table 表名
        :param sql df的源sql
        :return df
        '''
        # 转table
        if df is not None:
            df.createOrReplaceTempView(table)
        # 获得spark sql中table的df
        df = self.get_table_df(table)
        # 设为变量
        set_var(table, SparkDfProxy(df))
        # 流数据不缓存
        if df.isStreaming:
            if self.debug:
                df.printSchema()
            return df

        # 批量数据(非流数据)才缓存
        # 缓存
        if self.caching:
            # self.spark.sql(f"cache table {table}")
            df.cache()
        elif self.persisting: # 存储
            df.persist() # 默认是 MEMORY_AND_DISK_DESER
            self.persist_tables.add(table) # 记录存储的表
        # show
        if self.debug:
            df.explain()
            df.printSchema()
            df.show(20)

        return df

    # --- 读数据 ---
    # 读csv数据
    @replace_var_on_params
    def read_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_read('csv', False, config, default_options)

    # 读json数据
    @replace_var_on_params
    def read_json(self, config):
        self.do_read('json', False, config)

    # 读orc数据
    @replace_var_on_params
    def read_orc(self, config):
        self.do_read('orc', False, config)

    # 读parquet数据
    @replace_var_on_params
    def read_parquet(self, config):
        self.do_read('parquet', False, config)

    # 读文本数据
    @replace_var_on_params
    def read_text(self, config):
        self.do_read('text', False, config)

    # 读jdbc数据
    @replace_var_on_params
    def read_jdbc(self, config):
        self.do_read('jdbc', False, config)

    # 读table数据
    @replace_var_on_params
    def read_table(self, config):
        if isinstance(config, dict):
            tables = config.keys()
        elif isinstance(config, str):
            tables = [config]
        elif isinstance(config, list):
            tables = config
        else:
            raise Exception(f"无效read_table动作参数: {config}")
        for table in tables:
            self.on_load_df(None, table)

    # 执行读数据
    def do_read(self, type, is_stream, config, default_options = None):
        for table, option in config.items():
            # 修正read_csv()的参数
            if type == 'csv':
                if 'sep' in option and option['sep'] == '\\t':
                    option['sep'] = '\t'
            # 修正read_text()的参数
            sep = None
            if type == 'text':
                sep = get_and_del_dict_item(option, 'split')
                if sep == '\\t':
                    sep = '\t'
            if not option:
                option = {}
            if isinstance(option, str): # 路径
                if type == 'text':
                    key = 'paths'
                else:
                    key = 'path'
                option = {key: option}
            if type == 'text' and 'paths' not in option:
                option['paths'] = get_and_del_dict_item(option, 'path')
            if default_options:
                option = {**default_options, **option}
            # 加载数据到df
            #df = self.spark.read.csv(**option)
            if is_stream:
                read = self.spark.readStream
            else:
                read = self.spark.read
            df = getattr(read, type)(**option)
            # read_text()拆分单词
            if type == 'text' and sep is not None:
                df = df.select(explode(split(df.value, sep)).alias("word"))
            # 加载df后的处理
            df = self.on_load_df(df, table)

    # --- 读流数据 ---
    # 读csv流数据
    @replace_var_on_params
    def reads_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_read('csv', True, config, default_options)

    # 读json流数据
    @replace_var_on_params
    def reads_json(self, config):
        self.do_read('json', True, config)

    # 读orc流数据
    @replace_var_on_params
    def reads_orc(self, config):
        self.do_read('orc', True, config)

    # 读parquet流数据
    @replace_var_on_params
    def reads_parquet(self, config):
        self.do_read('parquet', True, config)

    # 读文本流数据
    @replace_var_on_params
    def reads_text(self, config):
        self.do_read('text', True, config)

    # 读socket流数据
    @replace_var_on_params
    def reads_socket(self, config):
        for table, host_port in config.items():
            host, port = host_port.split(':')
            # Create DataFrame representing the stream of input lines from connection to localhost:9999
            df = self.spark \
                .readStream \
                .format("socket") \
                .option("host", host) \
                .option("port", int(port)) \
                .load()
            # 加载df后的处理
            self.on_load_df(df, table)

    # 读模拟流数据
    @replace_var_on_params
    def reads_rate(self, config):
        for table, option in config.items():
            # Create DataFrame representing the stream of input lines from rate
            # 选项 rowsPerSecond
            if not isinstance(option, dict):
                option = {'rowsPerSecond': int(option)}
            df = self.spark.readStream \
                .format("rate") \
                .options(**option) \
                .load()
            # 加载df后的处理
            self.on_load_df(df, table)

    # 读kafka流数据
    # https://www.dandelioncloud.cn/article/details/1517520126281904129
    @replace_var_on_params
    def reads_kafka(self, config):
        for table, option in config.items():
            # Create DataFrame representing the stream of input lines from kafka
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", get_and_del_dict_item(option, 'brokers')) \
                .option("subscribe", get_and_del_dict_item(option, 'topic')) \
                .options(**option) \
                .load()
            # 加载df后的处理
            self.on_load_df(df, table)

    # --- 写数据 ---
    # 写csv数据
    @replace_var_on_params
    def write_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_write_method('csv', False, config, default_options)

    # 写json数据
    @replace_var_on_params
    def write_json(self, config):
        self.do_write_method('json', False, config)

    # 写orc数据
    @replace_var_on_params
    def write_orc(self, config):
        self.do_write_method('orc', False, config)

    # 写parquet数据
    @replace_var_on_params
    def write_parquet(self, config):
        self.do_write_method('parquet', False, config)

    # 写文本数据
    @replace_var_on_params
    def write_text(self, config):
        self.do_write_method('text', False, config)

    # 写jdbc数据
    @replace_var_on_params
    def write_jdbc(self, config):
        self.do_write_method('jdbc', False, config)

    # 执行写数据的方法
    def do_write_method(self, type, is_stream, config, default_options = None):
        for table, option in config.items():
            if isinstance(option, str): # 路径
                option = {'path': option}
            if default_options:
                option = {**default_options, **option}
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            # 输出df
            #df.write.csv(**option)
            if is_stream:
                write = df.writeStream
                outputMode = get_and_del_dict_item(option, 'outputMode')
            else:
                write = df.write
            # 方法调用，如 csv/json/orc/parquet/text/jdbc
            # 参考 /home/shi/.local/lib/python3.7/site-packages/pyspark/sql/streaming/readwriter.py
            writer = getattr(write, type)(**option)
            if is_stream:
                if outputMode is not None:
                    writer.outputMode(outputMode)
                self.start_swriter(writer)

    # 写table数据
    @replace_var_on_params
    def write_table(self, config):
        for table, option in config.items():
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            # 存为表
            df.write.saveAsTable(table, **option)

    # 执行写数据的格式链式调用
    def do_write_format(self, format, is_stream, config):
        for table, option in config.items():
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            # 输出df
            if is_stream:
                write = df.writeStream
                outputMode = get_and_del_dict_item(option, 'outputMode')
            else:
                write = df.write
                outputMode = get_and_del_dict_item(option, 'mode')
            writer = write.format(format) \
                .options(option)
            if is_stream: # 流处理
                if outputMode is not None:
                    writer.outputMode(outputMode)
                self.start_swriter(writer)
            else: # 批处理
                writer.mode(outputMode) \
                    .save()


    # --- 写流数据，选项都有 checkpointLocation ---
    # 写csv流数据
    @replace_var_on_params
    def writes_csv(self, config):
        default_options = {
            'header': True
        }
        self.do_write_method('csv', True, config, default_options)

    # 写json流数据
    @replace_var_on_params
    def writes_json(self, config):
        self.do_write_method('json', True, config)

    # 写orc流数据
    @replace_var_on_params
    def writes_orc(self, config):
        self.do_write_method('orc', True, config)

    # 写parquet流数据
    @replace_var_on_params
    def writes_parquet(self, config):
        self.do_write_method('parquet', True, config)

    # 写文本流数据
    @replace_var_on_params
    def writes_text(self, config):
        self.do_write_method('text', True, config)

    # 写console流数据
    @replace_var_on_params
    def writes_console(self, config):
        for table, option in config.items():
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            swriter = df.writeStream \
                .format("console") \
                .outputMode(get_and_del_dict_item(option, 'outputMode')) \
                .options(**option)
            self.start_swriter(swriter)

    # 写kafka流数据
    @replace_var_on_params
    def writes_kafka(self, config):
        for table, option in config.items():
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            swriter = df.writeStream \
                .format("kafka") \
                .outputMode(get_and_del_dict_item(option, 'outputMode')) \
                .option("kafka.bootstrap.servers", get_and_del_dict_item(option, 'brokers')) \
                .option("topic", get_and_del_dict_item(option, 'topic')) \
                .options(**option)
            self.start_swriter(swriter)

    '''
    # 写memory流数据
    @replace_var_on_params
    def writes_memory(self, config):
        for table, option in config.items():
            # 获得df
            df_proxy = get_var(table)
            df = df_proxy.df
            swriter = df.writeStream \
                .format("memory") \
                .outputMode(get_and_del_dict_item(option, 'outputMode')) \
                .options(**option) \
                .queryName(?)
            self.start_swriter(swriter)
    '''

    # 启动流writer
    def start_swriter(self, swriter):
        squery = swriter.start() # 启动
        if self.debug:
            squery.explain()
        self.squeries.append(squery)

    # 执行完的后置处理, 要在统计扫尾前调用
    def on_end(self):
        # stream query等待结束
        for squery in self.squeries:
            squery.awaitTermination()
        # 取消存储
        if self.persist_tables:
            log.debug(f"取消存储: {self.persist_tables}")
            for table in self.persist_tables:
                df_proxy = get_var(table)
                df = df_proxy.df
                df.unpersist()

    # --- 生成作业文件 ---
    # 生成要提交的作业文件+命令
    def generate_submiting_files(self, output, step_files, udf_file):
        if not os.path.exists(output):
            os.mkdir(output)
        # 1 生成入口文件run.py
        dir = os.path.dirname(__file__)
        copyfile(os.path.join(dir, 'run.py'), os.path.join(output, 'run.py'))

        # 2 复制udf文件
        copyfile(udf_file, os.path.join(output, os.path.basename(udf_file)))

        # 3 复制步骤文件
        files = []
        for src in step_files:
            filename = os.path.basename(src)
            files.append(filename)
            copyfile(src, os.path.join(output, filename))

        # 4 生成命令
        files = ','.join(files)
        cmd = f'''#根据真实环境修正master参数
spark-submit --master local|yarn|spark://127.0.0.1:7077 \\
    --driver-memory 1g \\
    --executor-memory 1g \\
    --files {files} \\
    run.py {files}''' # python命令后面不能接spark-submit参数了,它会被认为是python参数
        if udf_file is not None:
            cmd = f'''{cmd} -u {udf_file} \\
    --py-files {udf_file}'''
        #print("生成提交命令: " + cmd)
        write_file(os.path.join(output, 'submit.sh'), cmd)
        log.info("生成作业文件到目录: " + output)

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
        boot.generate_submiting_files(option.output, step_files, option.udf)
        return
    try:
        # 加载udf函数
        if option.udf != None:
            boot.udfs = load_module_funs(option.udf)
        # 执行yaml配置的步骤
        boot.run(step_files)
    except Exception as ex:
        log.error(f"Exception occurs: current step file is %s", boot.step_file, exc_info = ex)
        raise ex

if __name__ == '__main__':
    main()