import pandas as pd

def takePandas(self, n):
    rows = self.take(n)
    df = pd.DataFrame(rows, columns=self.columns)
    return df

from pyspark.sql import DataFrame
DataFrame.takePandas = takePandas

def save_intermediary_step(df, path,mode="overwrite"):
        df.write.parquet(path).mode(mode)

def createSparkSesion():
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder 
        .appName("Analises") 
        .getOrCreate())
    
    print(spark)
    
    return spark