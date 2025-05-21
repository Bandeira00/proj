def takePandas(self, n=5):
    import pandas as pd

    df_rows = self.take(n)
    df = pd.DataFrame(df_rows, columns=self.columns)

    return df

def countNulls(self):
    from pyspark.sql.functions import col, when, count
    df_nulls = self.select([count(when(col(c).isNull(), c)).alias(c) for c in self.columns]).takePandas()
    return df_nulls

from pyspark.sql import DataFrame
DataFrame.takePandas = takePandas
DataFrame.count_nulls = countNulls

def createSparkSesion():
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder 
        .appName("Analises") 
        .config("spark.driver.memory", "30g")
        .config("spark.jars","/home/gabriel/Documents/jars/postgresql-42.7.5.jar")
        .getOrCreate())
    
    print(spark)
    
    return spark

def save_intermediary_step(df, path,mode="overwrite"):
        df.write.parquet(path).mode(mode)




