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

// 3.1. Promedios de matemática > 10 ordenados descendentemente
println("=== Tabla 1: ===")
spark.sql("""
  SELECT INSTITUCION_EDUCATIVA, PROMEDIO_X_SECCION
  FROM matematica
  WHERE PROMEDIO_X_SECCION > 10
  ORDER BY PROMEDIO_X_SECCION DESC
""").show()

// 3.2. Textos: GRADO con promedio mayor a 12 ordenado ascendente
println("=== Tabla 2: ===")
spark.sql("""
  SELECT GRADO, PROMEDIO_X_SECCION
  FROM textos
  WHERE PROMEDIO_X_SECCION > 12
  ORDER BY GRADO ASC
""").show()

// 3.3. Instituciones con turno específico ordenadas por sección
println("=== Tabla 3: ===")
spark.sql("""
  SELECT INSTITUCION_EDUCATIVA, SECCION, PROMEDIO_X_SECCION
  FROM textos
  WHERE TURNO = 1.0
  ORDER BY SECCION
""").show()
