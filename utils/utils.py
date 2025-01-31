def takePandas(self, n=5):
    import pandas as pd

    df_rows = self.take(n)
    df = pd.DataFrame(df_rows, columns=self.columns)

    return df

from pyspark.sql import DataFrame
DataFrame.takePandas = takePandas

def createSparkSesion():
    from pyspark.sql import SparkSession
    
    spark = (SparkSession.builder 
        .appName("Analises") 
        .config("spark.driver.memory", "30g")
        .getOrCreate())
    
    print(spark)
    
    return spark

def save_intermediary_step(df, path,mode="overwrite"):
        df.write.parquet(path).mode(mode)
