package ru.test

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters._
import scala.util.Try

object EventParser {

  private val Cp1251: Charset = Charset.forName("windows-1251")
  private val Utf8: Charset = StandardCharsets.UTF_8
  private val TsFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  def readLines(path: Path): List[String] = {
    val cp1251Attempt = Try(Files.readAllLines(path, Cp1251).asScala.toList)
    cp1251Attempt.getOrElse(Files.readAllLines(path, Utf8).asScala.toList)
  }

  def parseFile(path: Path): Seq[Event] = {
    val sourceFile = path.toAbsolutePath.toString
    val lines = readLines(path)
    parseLines(lines, sourceFile)
  }

  def parseLines(lines: List[String], sourceFile: String): Seq[Event] = {
    val events = scala.collection.mutable.ArrayBuffer.empty[Event]
    var i = 0

    while (i < lines.length) {
      val raw = lines(i)
      val line = raw.trim

      if (line.isEmpty) {
        i += 1
      } else if (line.startsWith("SESSION_START ")) {
        parseSessionStart(line, sourceFile) match {
          case Some(event) => events += event
          case None => events += BrokenEvent(Seq(raw), "Cannot parse SESSION_START", sourceFile)
        }
        i += 1

      } else if (line.startsWith("SESSION_END ")) {
        parseSessionEnd(line, sourceFile) match {
          case Some(event) => events += event
          case None => events += BrokenEvent(Seq(raw), "Cannot parse SESSION_END", sourceFile)
        }
        i += 1

      } else if (line.startsWith("DOC_OPEN ")) {
        parseDocOpen(line, sourceFile) match {
          case Some(event) => events += event
          case None => events += BrokenEvent(Seq(raw), "Cannot parse DOC_OPEN", sourceFile)
        }
        i += 1

      } else if (line.startsWith("QS ")) {
        val (eventOpt, nextIndex) = parseQuickSearch(lines, i, sourceFile)
        eventOpt match {
          case Some(event) => events += event
          case None => events += BrokenEvent(Seq(raw), "Cannot parse QS block", sourceFile)
        }
        i = nextIndex

      } else if (line.startsWith("CARD_SEARCH_START ")) {
        val (eventOpt, nextIndex) = parseCardSearch(lines, i, sourceFile)
        eventOpt match {
          case Some(event) => events += event
          case None => events += BrokenEvent(Seq(raw), "Cannot parse CARD_SEARCH block", sourceFile)
        }
        i = nextIndex

      } else {
        i += 1
      }
    }

    events.toSeq
  }

  private def parseTimestamp(raw: String): Option[LocalDateTime] =
    Try(LocalDateTime.parse(raw, TsFormatter)).toOption

  private def parseSessionStart(line: String, sourceFile: String): Option[SessionStartEvent] = {
    val parts = line.split("\\s+")
    if (parts.length >= 2) {
      parseTimestamp(parts(1)).map(ts => SessionStartEvent(ts, sourceFile))
    } else None
  }

  private def parseSessionEnd(line: String, sourceFile: String): Option[SessionEndEvent] = {
    val parts = line.split("\\s+")
    if (parts.length >= 2) {
      parseTimestamp(parts(1)).map(ts => SessionEndEvent(ts, sourceFile))
    } else None
  }

  private def parseDocOpen(line: String, sourceFile: String): Option[DocOpenEvent] = {
    val parts = line.split("\\s+")
    if (parts.length >= 4) {
      for {
        ts <- parseTimestamp(parts(1))
      } yield DocOpenEvent(
        timestamp = ts,
        searchId = parts(2),
        docId = parts(3),
        sourceFile = sourceFile
      )
    } else None
  }

  private def parseQuickSearch(
                                lines: List[String],
                                startIndex: Int,
                                sourceFile: String
                              ): (Option[QuickSearchEvent], Int) = {
    val firstLine = lines(startIndex).trim

    val firstLineParts = firstLine.split("\\s+", 3)
    if (firstLineParts.length < 3) {
      return (None, startIndex + 1)
    }

    val timestampOpt = parseTimestamp(firstLineParts(1))
    val queryText = firstLineParts(2)

    val nextIndex = startIndex + 1
    if (nextIndex >= lines.length) {
      return (None, nextIndex)
    }

    val resultLine = lines(nextIndex).trim
    val resultParts = resultLine.split("\\s+")

    if (timestampOpt.isEmpty || resultParts.length < 2) {
      return (None, nextIndex + 1)
    }

    val searchId = resultParts.head
    val docs = resultParts.tail.toSeq

    (
      Some(
        QuickSearchEvent(
          timestamp = timestampOpt.get,
          queryText = queryText,
          searchId = searchId,
          resultDocs = docs,
          sourceFile = sourceFile
        )
      ),
      nextIndex + 1
    )
  }

  private def parseCardSearch(
                               lines: List[String],
                               startIndex: Int,
                               sourceFile: String
                             ): (Option[CardSearchEvent], Int) = {
    val startLine = lines(startIndex).trim
    val startParts = startLine.split("\\s+", 2)

    if (startParts.length < 2) {
      return (None, startIndex + 1)
    }

    val timestampOpt = parseTimestamp(startParts(1))
    if (timestampOpt.isEmpty) {
      return (None, startIndex + 1)
    }

    var i = startIndex + 1
    val rawCardLines = scala.collection.mutable.ArrayBuffer.empty[String]
    var foundEnd = false

    while (i < lines.length && !foundEnd) {
      val line = lines(i).trim
      if (line == "CARD_SEARCH_END") {
        foundEnd = true
      } else {
        rawCardLines += lines(i)
      }
      i += 1
    }

    if (!foundEnd || i >= lines.length) {
      return (None, i)
    }

    while (i < lines.length && lines(i).trim.isEmpty) {
      i += 1
    }

    if (i >= lines.length) {
      return (None, i)
    }

    val resultLine = lines(i).trim
    val resultParts = resultLine.split("\\s+")

    if (resultParts.length < 2) {
      return (None, i + 1)
    }

    val searchId = resultParts.head
    val docs = resultParts.tail.toSeq

    (
      Some(
        CardSearchEvent(
          timestamp = timestampOpt.get,
          rawCardLines = rawCardLines.toSeq,
          searchId = searchId,
          resultDocs = docs,
          sourceFile = sourceFile
        )
      ),
      i + 1
    )
  }
}