# Project with Spark using Scala

The dataset used is from the following page:

```txt
https://www.datosabiertos.gob.pe/dataset/resultados-de-evaluaci%C3%B3n-censal-regional-nivel-primaria-de-los-distritos-de-ventanilla-y-mi
```

Command to run files

```sh
start-spark-shell -i scripts/2_A_1.scala
```

Command to load the data into the database:

```sh
start-spark-shell --jars lib/postgresql-42.7.6.jar -i upload_grouped_csvs_to_postgres.scala
```
