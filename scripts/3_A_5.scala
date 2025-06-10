import org.apache.spark.sql.functions._

val dfMatematica = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/midb")
  .option("dbtable", "matematica")
  .option("user", "admin")
  .option("password", "admin123")
  .option("driver", "org.postgresql.Driver")
  .load()

val dfTextos = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/midb")
  .option("dbtable", "comprension_textos")
  .option("user", "admin")
  .option("password", "admin123")
  .option("driver", "org.postgresql.Driver")
  .load()

dfTextos.groupBy("GRADO")
  .agg(avg("PROMEDIO_X_SECCION").alias("PROMEDIO_POR_GRADO"))
  .show()
