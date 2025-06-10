val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 36)

val porIE = data.map(cols => (cols(2), cols(36).toDouble)).cache() // 1. map

val mediaYCount = porIE
  .mapValues(v => (v, 1))                                          // 2. mapValues
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))                // 3. reduceByKey
  .mapValues { case (sum, count) => (sum / count, count) }         // 4. mapValues

val joined = porIE.join(mediaYCount)                               // 5. join

val desviaciones = joined
  .mapValues { case (valor, (media, _)) =>
    val diff = valor - media
    (diff * diff, 1)
  }
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))                // 6. reduceByKey
  .mapValues { case (sumaDiff2, n) => math.sqrt(sumaDiff2 / n) }   // 7. mapValues

val conAltaDesv = desviaciones.filter(_._2 >= 5.0)                 // 8. filtro
val conteoAltaDesv = conAltaDesv.map(_ => ("alto", 1)).reduceByKey(_ + _) // 9. reduceByKey

println("Desviación estándar por institución:")
desviaciones.collect().foreach(println)
println("Instituciones con alta variabilidad: " + conteoAltaDesv.collect().mkString(", "))
