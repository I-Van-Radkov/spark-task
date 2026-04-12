package ru.test

import java.time.LocalDateTime

sealed trait Event {
  def sourceFile: String
}

final case class SessionStartEvent(
                                    timestamp: LocalDateTime,
                                    sourceFile: String
                                  ) extends Event

final case class SessionEndEvent(
                                  timestamp: LocalDateTime,
                                  sourceFile: String
                                ) extends Event

final case class QuickSearchEvent(
                                   timestamp: LocalDateTime,
                                   queryText: String,
                                   searchId: String,
                                   resultDocs: Seq[String],
                                   sourceFile: String
                                 ) extends Event

final case class CardSearchEvent(
                                  timestamp: LocalDateTime,
                                  rawCardLines: Seq[String],
                                  searchId: String,
                                  resultDocs: Seq[String],
                                  sourceFile: String
                                ) extends Event

final case class DocOpenEvent(
                               timestamp: LocalDateTime,
                               searchId: String,
                               docId: String,
                               sourceFile: String
                             ) extends Event

final case class BrokenEvent(
                              rawLines: Seq[String],
                              reason: String,
                              sourceFile: String
                            ) extends Event