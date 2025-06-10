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

// Join por INSTITUCION_EDUCATIVA
val joined = dfMatematica.join(dfTextos, Seq("INSTITUCION_EDUCATIVA"))

// 6.1 Mostrar promedios de ambas asignaturas
joined.select("INSTITUCION_EDUCATIVA", "PROMEDIO_X_SECCION").show()

// 6.2 Filtrar instituciones donde el promedio en ambos sea mayor a 15
joined.filter(dfMatematica("PROMEDIO_X_SECCION") > 15 && dfTextos("PROMEDIO_X_SECCION") > 15).show()

// 6.3 Calcular promedio combinado por institución
joined.withColumn("PROMEDIO_TOTAL", 
    (dfMatematica("PROMEDIO_X_SECCION") + dfTextos("PROMEDIO_X_SECCION")) / 2)
  .select("INSTITUCION_EDUCATIVA", "PROMEDIO_TOTAL")
  .show()
