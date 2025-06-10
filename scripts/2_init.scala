val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", ",")
  .csv("data/clean/1ero_comprension_textos.csv")
  .cache()

// ✅ Mostrar las primeras filas para verificar
df.show(5)

// ✅ Mostrar el esquema inferido
df.printSchema()
