package ru.test

import org.apache.spark.rdd.RDD

import java.time.format.DateTimeFormatter
import java.util.Locale

import scala.collection.mutable

object TaskSolver {

  private val TargetDocumentId = "ACC_45616"
  private val DocumentParameterId = "0"
  private val OutputDayFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  private sealed trait SearchKind
  private case object QuickSearchKind extends SearchKind
  private case object CardSearchKind extends SearchKind

  private final case class SearchContext(
    kind: SearchKind,
    resultDocs: Set[String]
  )

  def solve(sessionLogs: RDD[SessionLog]): ComputationResult = {
    val sessionMetrics = sessionLogs.map(analyzeSession).cache()

    val task1Count = sessionMetrics.map(_.task1Count).fold(0L)(_ + _)

    val task2Rows = sessionMetrics
      .flatMap(_.quickSearchDocOpens)
      .reduceByKey(_ + _)
      .sortBy { case (dailyOpen, _) => (dailyOpen.day.toEpochDay, dailyOpen.docId) }
      .collect()
      .toVector

    val brokenEventCount = sessionMetrics.map(_.brokenEventCount).fold(0L)(_ + _)

    sessionMetrics.unpersist(blocking = false)

    ComputationResult(
      task1Count = task1Count,
      task2Rows = task2Rows,
      brokenEventCount = brokenEventCount
    )
  }

  def analyzeSession(sessionLog: SessionLog): SessionMetrics = {
    val searchContexts = mutable.Map.empty[String, SearchContext]
    val quickSearchDocOpens = Vector.newBuilder[(DailyDocumentOpen, Long)]

    var task1Count = 0L

    sessionLog.events.foreach {
      case quickSearch: QuickSearchEvent =>
        searchContexts.update(
          quickSearch.searchId,
          SearchContext(QuickSearchKind, quickSearch.resultDocs)
        )

      case cardSearch: CardSearchEvent =>
        if (containsTargetCardDocument(cardSearch)) {
          task1Count += 1L
        }

        searchContexts.update(
          cardSearch.searchId,
          SearchContext(CardSearchKind, cardSearch.resultDocs)
        )

      case docOpen: DocOpenEvent =>
        searchContexts.get(docOpen.searchId) match {
          case Some(SearchContext(QuickSearchKind, resultDocs)) if resultDocs.contains(docOpen.docId) =>
            quickSearchDocOpens += DailyDocumentOpen(
              day = docOpen.timestamp.toLocalDate,
              docId = docOpen.docId
            ) -> 1L

          case _ =>
        }

      case _: SessionStartEvent =>
      case _: SessionEndEvent =>
    }

    SessionMetrics(
      task1Count = task1Count,
      quickSearchDocOpens = quickSearchDocOpens.result(),
      brokenEventCount = sessionLog.brokenEvents.size.toLong
    )
  }

  def renderTask2Rows(task2Rows: Vector[(DailyDocumentOpen, Long)]): Vector[String] =
    task2Rows.map { case (dailyOpen, openCount) =>
      val day = OutputDayFormatter.format(dailyOpen.day)
      s"$day,${dailyOpen.docId},$openCount"
    }

  private def containsTargetCardDocument(cardSearch: CardSearchEvent): Boolean =
    cardSearch.parameters.exists { parameter =>
      parameter.parameterId == DocumentParameterId &&
      normalizeDocumentId(parameter.value) == TargetDocumentId
    }

  private def normalizeDocumentId(raw: String): String =
    raw.trim.toUpperCase(Locale.ROOT)
}
