package ru.test

import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.jdk.CollectionConverters._
import scala.util.Using

object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsultantSparkTask")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val inputDir = Paths.get("data/sessions")
    val outputDir = Paths.get("results")

    val files =
      if (Files.exists(inputDir) && Files.isDirectory(inputDir)) {
        Using.resource(Files.list(inputDir)) { stream =>
          stream.iterator().asScala
            .filter(Files.isRegularFile(_))
            .toList
        }
      } else {
        throw new IllegalArgumentException(s"Input directory not found: $inputDir")
      }

    val allEvents = files.flatMap(EventParser.parseFile)

    val quickSearches = allEvents.collect { case e: QuickSearchEvent => e }
    val cardSearches  = allEvents.collect { case e: CardSearchEvent => e }
    val docOpens      = allEvents.collect { case e: DocOpenEvent => e }

    val quickSearchesDf = quickSearches.toDF()
    val cardSearchesDf  = cardSearches.toDF()
    val docOpensDf      = docOpens.toDF()

    val task1Result = TaskSolver.solveTask1(cardSearchesDf)
    val task2Df = TaskSolver.solveTask2(quickSearchesDf, docOpensDf)

    saveResults(outputDir, task1Result, task2Df)

    println(s"Results successfully saved to: ${outputDir.toAbsolutePath}")

    spark.stop()
  }

  private def saveResults(
                           outputDir: Path,
                           task1Result: Long,
                           task2Df: org.apache.spark.sql.DataFrame
                         ): Unit = {

    if (!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
    }

    saveTask1(outputDir.resolve("task1.txt"), task1Result)
    saveTask2(outputDir.resolve("task2.csv"), task2Df)
  }

  private def saveTask1(path: Path, task1Result: Long): Unit = {
    val content =
      s"Количество раз, когда в карточке производили поиск документа ACC_45616: $task1Result"

    Files.writeString(
      path,
      content,
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  }

  private def saveTask2(path: Path, task2Df: org.apache.spark.sql.DataFrame): Unit = {
    val rows = task2Df.collect()

    val lines =
      ("day,docId,open_count" +:
        rows.map { row =>
          val day = row.getAs[String]("day")
          val docId = row.getAs[String]("docId")
          val openCount = row.getAs[Long]("open_count")
          s"$day,$docId,$openCount"
        }).mkString(System.lineSeparator())

    Files.writeString(
      path,
      lines,
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  }
}