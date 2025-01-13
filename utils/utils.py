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
        .getOrCreate())
    
    print(spark)
    
    return spark

def save_intermediary_step(df, path,mode="overwrite"):
        df.write.parquet(path).mode(mode)

class Dog:
    """ Blueprint of a dog """
    # class variable shared by all instances
    #just to remender about classes use case , out of practice
    species = ["canis lupus"]
    def __init__(self, name, color):
        self.name = name
        self.state = "sleeping"
        self.color = color
    def command(self, x):
        if x == self.name:
            self.bark(2)
        elif x == "sit":
            self.state = "sit"
        else:
            self.state = "wag tail"
    def bark(self, freq):
        for i in range(freq):
            print("[" + self.name+ "]: Woof!")
