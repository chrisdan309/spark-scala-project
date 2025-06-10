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

dfMatematica.createOrReplaceTempView("matematica")
dfTextos.createOrReplaceTempView("textos")


// 1.1. Join básico por institución
println("=== Tabla 1: Resultados JOIN ===")
spark.sql("""
  SELECT m.INSTITUCION_EDUCATIVA, m.PROMEDIO_X_SECCION AS promedio_mate, 
         t.PROMEDIO_X_SECCION AS promedio_texto
  FROM matematica m
  JOIN textos t
  ON m.INSTITUCION_EDUCATIVA = t.INSTITUCION_EDUCATIVA
""").show()

// 1.2. Join con condición de filtro
println("=== Tabla 2: Estadísticas por GRADO ===")
spark.sql("""
  SELECT m.INSTITUCION_EDUCATIVA, m.GRADO, t.SECCION
  FROM matematica m
  JOIN textos t ON m.INSTITUCION_EDUCATIVA = t.INSTITUCION_EDUCATIVA
  WHERE m.PROMEDIO_X_SECCION > 15 AND t.PROMEDIO_X_SECCION > 15
""").show()

// 1.3. Join y cálculo de promedio combinado
println("=== Tabla 3: Agrupación por SECCION ===")
spark.sql("""
  SELECT m.INSTITUCION_EDUCATIVA,
         ROUND((m.PROMEDIO_X_SECCION + t.PROMEDIO_X_SECCION)/2, 2) AS promedio_total
  FROM matematica m
  JOIN textos t ON m.INSTITUCION_EDUCATIVA = t.INSTITUCION_EDUCATIVA
""").show()
