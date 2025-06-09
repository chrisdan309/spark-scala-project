import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Spark + PostgreSQL")
      .master("local[*]")
      .getOrCreate()

    val jdbcUrl = "jdbc:postgresql://localhost:5432/midb"
    val dbUser = "admin"
    val dbPass = "admin123"

    val empleadosDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "empleados")
      .option("user", dbUser)
      .option("password", dbPass)
      .load()

    val departamentosDF = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "departamentos")
      .option("user", dbUser)
      .option("password", dbPass)
      .load()

    empleadosDF.show()
    departamentosDF.show()

    val empleadosConDepto = empleadosDF
      .join(departamentosDF, empleadosDF("departamento_id") === departamentosDF("id"))
      .select(
        empleadosDF("nombre").alias("empleado"),
        empleadosDF("salario"),
        departamentosDF("nombre").alias("departamento")
      )

    empleadosConDepto.show()

    empleadosDF.filter("salario > 3000").show()

    empleadosDF.select("nombre", "salario").show()

    empleadosDF.orderBy(col("salario").desc).show()

    empleadosDF.groupBy("departamento_id").count().show()

    empleadosDF.groupBy("departamento_id")
      .agg(avg("salario").alias("promedio_salario"))
      .show()

    empleadosDF.groupBy("departamento_id")
      .agg(
        max("salario").alias("salario_max"),
        min("salario").alias("salario_min")
      ).show()

    empleadosDF.createOrReplaceTempView("empleados")
    departamentosDF.createOrReplaceTempView("departamentos")

    val resultado = spark.sql(
      """
        |SELECT e.nombre, e.salario, d.nombre AS departamento
        |FROM empleados e
        |JOIN departamentos d ON e.departamento_id = d.id
        |WHERE e.salario > 3000
        |""".stripMargin)

    resultado.show()

    println("Presiona ENTER para finalizar...")
    scala.io.StdIn.readLine()

    spark.stop()
  }
}
