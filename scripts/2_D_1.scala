val raw = sc.textFile("data/clean/1ero_comprension_textos.csv")
val header = raw.first()
val data = raw.filter(_ != header).map(_.split(",")).filter(_.length > 36)

val porTurnoBase = data
  .map(cols => (cols(5), cols(36).toDouble))            // 1. map: (turno, valor)

val sumYCount = porTurnoBase
  .mapValues(v => (v, 1))                               // 2. mapValues: (turno, (valor, 1))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))    // 3. reduceByKey: suma y conteo

val promedios = sumYCount
  .mapValues { case (suma, count) => BigDecimal(suma / count).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble }

val conteoSuperanUmbral = promedios
  .filter { case (_, promedio) => promedio > 15.0 }     // 4. filtro
  .map(_ => ("superan", 1))                             // 5. map a constante
  .reduceByKey(_ + _)                                   // 6. reduceByKey: contar cuántos

println("Promedios por turno:")
promedios.collect().foreach(println)
println("Turnos que superan 15.0: " + conteoSuperanUmbral.collect().mkString(", "))
