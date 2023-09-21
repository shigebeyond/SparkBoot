# spark-submit提交的python入口文件
import sys
from SparkBoot import Boot

if __name__ == '__main__':
    # 执行yaml配置的步骤
    step_file = sys.argv[1]
    Boot().run([step_file])