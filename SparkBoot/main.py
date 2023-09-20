# spark-submit提交的python入口文件
import sys
from SparkBoot import Boot

if __name__ == '__main__':
    # 执行yaml配置的步骤
    Boot().run(sys.argv[1])