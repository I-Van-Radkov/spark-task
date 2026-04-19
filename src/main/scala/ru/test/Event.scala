package ru.test

import java.time.{LocalDate, LocalDateTime}

sealed trait Event {
  def timestamp: LocalDateTime
}

sealed trait SearchEvent extends Event {
  def searchId: String
  def resultDocs: Set[String]
}

final case class SearchParameter(
  parameterId: String,
  value: String
)

final case class SessionStartEvent(
  timestamp: LocalDateTime
) extends Event

final case class SessionEndEvent(
  timestamp: LocalDateTime
) extends Event

final case class QuickSearchEvent(
  timestamp: LocalDateTime,
  queryText: String,
  searchId: String,
  resultDocs: Set[String]
) extends SearchEvent

final case class CardSearchEvent(
  timestamp: LocalDateTime,
  parameters: Vector[SearchParameter],
  rawCardLines: Vector[String],
  searchId: String,
  resultDocs: Set[String]
) extends SearchEvent

final case class DocOpenEvent(
  timestamp: LocalDateTime,
  searchId: String,
  docId: String
) extends Event

final case class BrokenEvent(
  reason: String,
  rawLines: Vector[String]
)

final case class SessionLog(
  sourceFile: String,
  events: Vector[Event],
  brokenEvents: Vector[BrokenEvent]
)

final case class DailyDocumentOpen(
  day: LocalDate,
  docId: String
)

final case class SessionMetrics(
  task1Count: Long,
  quickSearchDocOpens: Vector[(DailyDocumentOpen, Long)],
  brokenEventCount: Long
)

final case class ComputationResult(
  task1Count: Long,
  task2Rows: Vector[(DailyDocumentOpen, Long)],
  brokenEventCount: Long
)
