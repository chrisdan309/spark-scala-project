import org.apache.spark.sql.SparkSession
import java.io.File

val spark = SparkSession.builder()
  .appName("CargarCSVporTipoPostgreSQL")
  .master("local[*]")
  .getOrCreate()

// Ruta a la carpeta que contiene los CSV
val folder = new File("data/clean")
val files = folder.listFiles.filter(_.getName.endsWith(".csv"))

// Filtrar archivos por tipo
val matematicaFiles = files.filter(_.getName.endsWith("matematica.csv"))
val textosFiles = files.filter(_.getName.endsWith("textos.csv"))

// Función para leer y combinar múltiples archivos CSV
def readCSVFiles(fileArray: Array[File]) = {
  val paths = fileArray.map(_.getPath)
  spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(paths: _*)
}

// Leer y guardar los archivos de matemática
val dfMatematica = readCSVFiles(matematicaFiles)
dfMatematica.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/midb")
  .option("dbtable", "matematica")
  .option("user", "admin")
  .option("password", "admin123")
  .option("driver", "org.postgresql.Driver")
  .mode("overwrite")
  .save()

// Leer y guardar los archivos de textos
val dfTextos = readCSVFiles(textosFiles)
dfTextos.write
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/midb")
  .option("dbtable", "comprension_textos")
  .option("user", "admin")
  .option("password", "admin123")
  .option("driver", "org.postgresql.Driver")
  .mode("overwrite")
  .save()

spark.stop()
