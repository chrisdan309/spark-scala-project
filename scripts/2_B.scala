val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 35)

val porTurnoPromedios = data.map(cols =>
  (cols(5), cols(36).toDouble)
)

val maxPorTurno = porTurnoPromedios.reduceByKey((a, b) => math.max(a, b))
val minPorTurno = porTurnoPromedios.reduceByKey((a, b) => math.min(a, b))

println("Máximos por sección:")
maxPorTurno.collect().foreach(println)

println("Mínimos por sección:")
minPorTurno.collect().foreach(println)
