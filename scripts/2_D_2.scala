val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 36)

val porGrado = data
  .map(cols => (cols(3), cols(36).toDouble))                 // 1. map

val agrupado = porGrado.groupByKey()                         // 2. groupByKey

val estadisticas = agrupado.mapValues { iterable =>
  val list = iterable.toList
  val max = list.max
  val min = list.min
  val avg = list.sum / list.size
  (min, max, BigDecimal(avg).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
}                                                             // 3. mapValues con múltiples cálculos

val filtrado = estadisticas.filter { case (_, (_, max, _)) => max >= 20.0 }  // 4. filtro
val renombrado = filtrado.map { case (grado, stats) => (grado, stats._1, stats._2, stats._3) }  // 5. map plano

println("Estadísticas por GRADO (min, max, promedio):")
renombrado.collect().foreach(println)
