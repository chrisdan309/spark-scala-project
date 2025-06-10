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

// 7.1 round + alias
dfMatematica.select($"GRADO", round($"PROMEDIO_X_SECCION", 1).alias("PROMEDIO_REDONDEADO")).show()

// 7.2 max por turno
dfTextos.groupBy("TURNO").agg(max("PROMEDIO_X_SECCION").alias("MAXIMO")).show()

// 7.3 countDistinct por sección
dfTextos.groupBy("GRADO").agg(countDistinct("SECCION").alias("SECCIONES_DISTINTAS")).show()
