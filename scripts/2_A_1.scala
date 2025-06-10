val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 35)

val evaluadosPorGradoSeccionTurno = data.map(cols =>
  ((cols(3), cols(4), cols(5)), cols(7).toDouble)
).reduceByKey(_ + _)

evaluadosPorGradoSeccionTurno.take(10).foreach(println)
