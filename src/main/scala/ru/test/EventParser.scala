package ru.test

import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path}
import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

object EventParser {

  private val Cp1251: Charset = Charset.forName("windows-1251")
  private val Utf8: Charset = StandardCharsets.UTF_8

  private val CompactTimestampFormatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("dd.MM.yyyy_HH:mm:ss")

  private val VerboseTimestampFormatter: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .parseCaseInsensitive()
      .appendPattern("EEE,_d_MMM_yyyy_HH:mm:ss_Z")
      .toFormatter(Locale.ENGLISH)

  private val SearchParameterPattern = "^\\$([^\\s]+)\\s*(.*)$".r

  def parseFile(path: Path): SessionLog = {
    val lines = readLines(path)
    parseLines(lines, path.toAbsolutePath.toString)
  }

  def readLines(path: Path): List[String] = {
    val cp1251Attempt = Try(Files.readAllLines(path, Cp1251).asScala.toList)
    cp1251Attempt.getOrElse(Files.readAllLines(path, Utf8).asScala.toList)
  }

  def parseLines(lines: List[String], sourceFile: String): SessionLog = {
    val events = ArrayBuffer.empty[Event]
    val brokenEvents = ArrayBuffer.empty[BrokenEvent]

    var index = 0
    while (index < lines.length) {
      val rawLine = lines(index)
      val line = rawLine.trim

      if (line.isEmpty) {
        index += 1
      } else if (line.startsWith("SESSION_START ")) {
        parseSingleLineEvent(line, "SESSION_START")(SessionStartEvent.apply) match {
          case Some(event) => events += event
          case None => brokenEvents += BrokenEvent("Cannot parse SESSION_START", Vector(rawLine))
        }
        index += 1
      } else if (line.startsWith("SESSION_END ")) {
        parseSingleLineEvent(line, "SESSION_END")(SessionEndEvent.apply) match {
          case Some(event) => events += event
          case None => brokenEvents += BrokenEvent("Cannot parse SESSION_END", Vector(rawLine))
        }
        index += 1
      } else if (line.startsWith("DOC_OPEN ")) {
        parseDocOpen(line) match {
          case Some(event) => events += event
          case None => brokenEvents += BrokenEvent("Cannot parse DOC_OPEN", Vector(rawLine))
        }
        index += 1
      } else if (line.startsWith("QS ")) {
        val (eventOpt, nextIndex, rawBlock) = parseQuickSearch(lines, index)
        eventOpt match {
          case Some(event) => events += event
          case None => brokenEvents += BrokenEvent("Cannot parse QS block", rawBlock)
        }
        index = nextIndex
      } else if (line.startsWith("CARD_SEARCH_START ")) {
        val (eventOpt, nextIndex, rawBlock) = parseCardSearch(lines, index)
        eventOpt match {
          case Some(event) => events += event
          case None => brokenEvents += BrokenEvent("Cannot parse CARD_SEARCH block", rawBlock)
        }
        index = nextIndex
      } else {
        brokenEvents += BrokenEvent("Unknown line prefix", Vector(rawLine))
        index += 1
      }
    }

    SessionLog(
      sourceFile = sourceFile,
      events = events.toVector,
      brokenEvents = brokenEvents.toVector
    )
  }

  private def parseSingleLineEvent(
    line: String,
    prefix: String
  )(
    buildEvent: LocalDateTime => Event
  ): Option[Event] = {
    val timestampRaw = line.stripPrefix(prefix).trim
    parseTimestamp(timestampRaw).map(buildEvent)
  }

  private def parseDocOpen(line: String): Option[DocOpenEvent] = {
    val parts = line.split("\\s+")
    if (parts.length < 4) {
      None
    } else {
      parseTimestamp(parts(1)).map { timestamp =>
        DocOpenEvent(
          timestamp = timestamp,
          searchId = parts(2),
          docId = normalizeDocumentId(parts(3))
        )
      }
    }
  }

  private def parseQuickSearch(
    lines: List[String],
    startIndex: Int
  ): (Option[QuickSearchEvent], Int, Vector[String]) = {
    val firstLine = lines(startIndex).trim
    val firstLineParts = firstLine.split("\\s+", 3)

    if (firstLineParts.length < 2) {
      return (None, startIndex + 1, Vector(lines(startIndex)))
    }

    val queryText =
      if (firstLineParts.length >= 3) firstLineParts(2)
      else ""

    val nextIndex = startIndex + 1
    if (nextIndex >= lines.length) {
      return (None, nextIndex, Vector(lines(startIndex)))
    }

    val rawBlock = Vector(lines(startIndex), lines(nextIndex))

    val eventOpt = for {
      timestamp <- parseTimestamp(firstLineParts(1))
      (searchId, resultDocs) <- parseSearchResultLine(lines(nextIndex))
    } yield QuickSearchEvent(
      timestamp = timestamp,
      queryText = queryText,
      searchId = searchId,
      resultDocs = resultDocs
    )

    (eventOpt, nextIndex + 1, rawBlock)
  }

  private def parseCardSearch(
    lines: List[String],
    startIndex: Int
  ): (Option[CardSearchEvent], Int, Vector[String]) = {
    val startLine = lines(startIndex).trim
    val startParts = startLine.split("\\s+", 2)

    if (startParts.length < 2) {
      return (None, startIndex + 1, Vector(lines(startIndex)))
    }

    val timestampOpt = parseTimestamp(startParts(1))
    if (timestampOpt.isEmpty) {
      return (None, startIndex + 1, Vector(lines(startIndex)))
    }

    val rawBlock = ArrayBuffer(lines(startIndex))
    val rawCardLines = ArrayBuffer.empty[String]

    var index = startIndex + 1
    var foundEnd = false

    while (index < lines.length && !foundEnd) {
      rawBlock += lines(index)
      val line = lines(index).trim
      if (line == "CARD_SEARCH_END") {
        foundEnd = true
      } else {
        rawCardLines += lines(index)
      }
      index += 1
    }

    if (!foundEnd) {
      return (None, index, rawBlock.toVector)
    }

    while (index < lines.length && lines(index).trim.isEmpty) {
      rawBlock += lines(index)
      index += 1
    }

    if (index >= lines.length) {
      return (None, index, rawBlock.toVector)
    }

    rawBlock += lines(index)

    val eventOpt = for {
      (searchId, resultDocs) <- parseSearchResultLine(lines(index))
    } yield CardSearchEvent(
      timestamp = timestampOpt.get,
      parameters = parseSearchParameters(rawCardLines.toVector),
      rawCardLines = rawCardLines.toVector,
      searchId = searchId,
      resultDocs = resultDocs
    )

    (eventOpt, index + 1, rawBlock.toVector)
  }

  private def parseSearchResultLine(line: String): Option[(String, Set[String])] = {
    val parts = line.trim.split("\\s+").filter(_.nonEmpty)
    if (parts.isEmpty) {
      None
    } else {
      val searchId = parts.head
      val resultDocs = parts.tail.iterator.map(normalizeDocumentId).toSet
      Some(searchId -> resultDocs)
    }
  }

  private def parseSearchParameters(rawLines: Vector[String]): Vector[SearchParameter] =
    rawLines.collect {
      case SearchParameterPattern(parameterId, value) =>
        SearchParameter(parameterId = parameterId, value = value.trim)
    }

  private def parseTimestamp(raw: String): Option[LocalDateTime] =
    Try(LocalDateTime.parse(raw, CompactTimestampFormatter))
      .orElse(Try(LocalDateTime.from(VerboseTimestampFormatter.parse(raw))))
      .toOption

  private def normalizeDocumentId(raw: String): String =
    raw.trim.toUpperCase(Locale.ROOT)
}
