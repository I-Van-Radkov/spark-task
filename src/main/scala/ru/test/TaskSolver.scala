package ru.test

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TaskSolver {

  def solveTask1(cardSearchesDf: DataFrame): Long = {
    cardSearchesDf
      .filter(array_contains(col("resultDocs"), "ACC_45616"))
      .count()
  }

  def solveTask2(
                  quickSearchesDf: DataFrame,
                  docOpensDf: DataFrame
                ): DataFrame = {

    val quickSearchDocsDf = quickSearchesDf
      .withColumn("docId", explode(col("resultDocs")))
      .select(
        col("sourceFile"),
        col("searchId"),
        col("docId")
      )

    docOpensDf
      .withColumn("day", date_format(col("timestamp"), "dd.MM.yyyy"))
      .join(
        quickSearchDocsDf,
        Seq("sourceFile", "searchId", "docId"),
        "inner"
      )
      .groupBy(col("day"), col("docId"))
      .count()
      .withColumnRenamed("count", "open_count")
      .orderBy(col("day"), col("docId"))
  }
}