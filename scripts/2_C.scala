val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 36)

// Elegimos la columna: PROMEDIO_X_SECCION → índice 36
val valores = data.map(cols => cols(36).toDouble).filter(!_.isNaN).cache()

val count = valores.count()
val sum = valores.sum()
val avg = sum / count

// Mediana: ordenar y hallar el valor del medio
val sorted = valores.sortBy(x => x).zipWithIndex().map { case (v, i) => (i, v) }.cache()
val median = if (count % 2 == 0) {
  val mid1 = sorted.lookup(count / 2 - 1).head
  val mid2 = sorted.lookup(count / 2).head
  (mid1 + mid2) / 2.0
} else {
  sorted.lookup(count / 2).head
}

// Máximo y mínimo
val max = valores.max()
val min = valores.min()

// Desviación estándar
val sumSquaredDiffs = valores.map(x => math.pow(x - avg, 2)).sum()
val stdDev = math.sqrt(sumSquaredDiffs / count)

println(f"Estadísticas de PROMEDIO_X_SECCION:")
println(f"Promedio: $avg%.2f")
println(f"Mediana:  $median%.2f")
println(f"Mínimo:   $min%.2f")
println(f"Máximo:   $max%.2f")
println(f"Desv. estándar: $stdDev%.2f")
