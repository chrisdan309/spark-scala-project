val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 35)

val sumaP1aP3 = data.map(cols =>
  ((cols(1), cols(3), cols(4)), cols(8).toDouble + cols(9).toDouble + cols(10).toDouble)
).reduceByKey(_ + _)

sumaP1aP3.take(10).foreach(println)
