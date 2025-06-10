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

// 2.1. Contar secciones por grado
println("=== Tabla 1: ===")
spark.sql("""
  SELECT GRADO, COUNT(SECCION) AS total_secciones
  FROM matematica
  GROUP BY GRADO
""").show()

// 2.2. Contar instituciones por turno
println("=== Tabla 2: ===")
spark.sql("""
  SELECT TURNO, COUNT(DISTINCT INSTITUCION_EDUCATIVA) AS instituciones
  FROM textos
  GROUP BY TURNO
""").show()

// 2.3. Contar cuántas veces aparece cada sección en textos
println("=== Tabla 3: ===")
spark.sql("""
  SELECT SECCION, COUNT(*) AS frecuencia
  FROM textos
  GROUP BY SECCION
""").show()
