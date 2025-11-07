##################################################################################################################
# Archivo: transform_load_6.py
##################################################################################################################

# Importamos librerias
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    # --- Parámetros de HDFS ---
    NN_URI = "hdfs://172.17.0.2:9000"  # ajusta si tu NameNode/puerto son otros
    INPUT_FILE   = f"{NN_URI}/nifi/titanic.csv"
    WAREHOUSE   = f"{NN_URI}/user/hive/warehouse"

    # 1) Crear la sesión de Spark con soporte Hive y FS por defecto en HDFS
    spark = (
        SparkSession
            .builder
            .appName("transform_load_6")
            .enableHiveSupport()
            .config("spark.hadoop.fs.defaultFS", NN_URI)
            .config("spark.sql.warehouse.dir", WAREHOUSE)
            .getOrCreate()
    )

    # 2) Rutas de entrada (parquet desde HDFS)
    df = spark.read.option("header","true").csv(INPUT_FILE)

    # 3) TransformacioneS
    # Casteamos las columnas
    df = (df
    .withColumn("PassengerId", F.col("PassengerId").cast("int"))
    .withColumn("Survived",    F.col("Survived").cast("int"))
    .withColumn("Pclass",      F.col("Pclass").cast("int"))
    .withColumn("SibSp",       F.col("SibSp").cast("int"))
    .withColumn("Parch",       F.col("Parch").cast("int"))
    .withColumn("Age",         F.col("Age").cast("int"))
    .withColumn("Fare",        F.col("Fare").cast("double"))
    .withColumn("Name",        F.col("Name").cast("string"))
    .withColumn("Sex",         F.col("Sex").cast("string"))
    .withColumn("Ticket",      F.col("Ticket").cast("string"))
    .withColumn("Cabin",       F.col("Cabin").cast("string"))
    .withColumn("Embarked",    F.col("Embarked").cast("string"))
    )

    # Borramos las columnas 
    df = df.drop('SibSp', 'Parch')

    # Calculamos promedio de edad segun genero por cada fila (funciones de ventana edentro de Pyspark)
    w = Window.partitionBy("Sex")   # "M"/"F", "hombre"/"mujer", etc.
    df_out = df.withColumn("promedio_edad_genero", F.avg("Age").over(w))

    # Rellenamos con 0 si es nulo el contenido de 'Cabin'
    df_out = df_out.fillna({'Cabin': '0'})

    # (opcional) métricas rápidas al log
    print(f"[INFO] filas titanic_clean : {df_out.count()}")
    
    # 4) Load (escritura en HDFS en formato csv)
    df_out.select("passengerid","survived","pclass","name","sex","age","ticket","fare","cabin","embarked","promedio_edad_genero").write.mode("append").insertInto("titanic.titanic_clean")
    
    spark.stop()

if __name__ == "__main__":
    main()