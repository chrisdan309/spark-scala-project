val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 35)

val promedioPorIEGradoSeccion = data.map(cols =>
  ((cols(2), cols(3), cols(4)), (cols(32).toDouble, 1))
).reduceByKey {
  case ((sum1, c1), (sum2, c2)) => (sum1 + sum2, c1 + c2)
}.mapValues { case (sum, count) => sum / count }

promedioPorIEGradoSeccion.take(10).foreach(println)
