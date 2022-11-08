import pyspark.sql.functions as F
import pandas as pd
import pyspark.sql.functions as F
from pyspark.python.pyspark.shell import sqlContext

# Sample data
df = pd.DataFrame({'x1': ['a', '1', '2'],
                   'x2': ['b', None, '2'],
                   'x3': ['c', '0', '3'] })
df = sqlContext.createDataFrame(df)
df.show()

def drop_null_columns(df, min_none = 0):
    """
    This function drops all columns which contain null values.
    :param df: A PySpark DataFrame
    """
    null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in null_counts.items() if v > min_none]
    df = df.drop(*to_drop)
    return df.columns

print(drop_null_columns(df))