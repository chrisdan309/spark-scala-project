import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("CargarCSVaPostgreSQL")
  .master("local[*]")
  .getOrCreate()

val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/clean/1ero_matematica.csv")

df.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/midb")
  .option("dbtable", "matematicas")
  .option("user", "admin")
  .option("password", "admin123")
  .option("driver", "org.postgresql.Driver")
  .mode("overwrite")
  .save()

spark.stop()
