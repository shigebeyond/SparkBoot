from pyspark.sql.functions import udf
from pyspark.sql.types import *

@udf(returnType=DoubleType())
def add(m, n):
    return float(m) + float(n)

@udf(returnType=DoubleType())
def add_one(a):
    return float(a) + 1.0
