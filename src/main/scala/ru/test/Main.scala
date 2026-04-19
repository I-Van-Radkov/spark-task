package ru.test

import org.apache.spark.{SparkConf, SparkContext}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import scala.jdk.CollectionConverters._
import scala.util.Using

object Main {

  def main(args: Array[String]): Unit = {
    val inputDir = Paths.get("data/sessions")
    val outputDir = Paths.get("results")

    val sparkConf = new SparkConf()
      .setAppName("ConsultantSparkTask")
      .setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("ERROR")

    try {
      val inputFiles = listInputFiles(inputDir)
      val sessionLogs = sparkContext
        .parallelize(inputFiles, math.min(inputFiles.size, sparkContext.defaultParallelism).max(1))
        .map(path => EventParser.parseFile(Paths.get(path)))

      val result = TaskSolver.solve(sessionLogs)

      saveResults(outputDir, result)

      println(s"Task 1 result: ${result.task1Count}")
      println(s"Task 2 rows: ${result.task2Rows.size}")
      println(s"Broken events skipped: ${result.brokenEventCount}")
      println(s"Results saved to: ${outputDir.toAbsolutePath}")
    } finally {
      sparkContext.stop()
    }
  }

  private def listInputFiles(inputDir: Path): Vector[String] = {
    if (!Files.exists(inputDir) || !Files.isDirectory(inputDir)) {
      throw new IllegalArgumentException(s"Input directory not found: $inputDir")
    }

    Using.resource(Files.list(inputDir)) { stream =>
      stream.iterator().asScala
        .filter(Files.isRegularFile(_))
        .map(_.toAbsolutePath.toString)
        .toVector
        .sorted
    }
  }

  private def saveResults(outputDir: Path, result: ComputationResult): Unit = {
    if (!Files.exists(outputDir)) {
      Files.createDirectories(outputDir)
    }

    saveTask1(outputDir.resolve("task1.txt"), result.task1Count)
    saveTask2(outputDir.resolve("task2.csv"), result.task2Rows)
  }

  private def saveTask1(path: Path, task1Count: Long): Unit = {
    val content = s"Card searches with document ACC_45616: $task1Count"

    Files.writeString(
      path,
      content,
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  }

  private def saveTask2(path: Path, task2Rows: Vector[(DailyDocumentOpen, Long)]): Unit = {
    val header = "day,doc_id,open_count"
    val lines = header +: TaskSolver.renderTask2Rows(task2Rows)

    Files.writeString(
      path,
      lines.mkString(System.lineSeparator()),
      StandardCharsets.UTF_8,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING
    )
  }
}
