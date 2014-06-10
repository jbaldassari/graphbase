package graphbase

import java.util.Date

/** Trait for metrics stores */
trait MetricsStore {

	/** Time information returned to Graphite */
	case class TimeInfo(val start: Date, val end: Date, val step: Long)
	
	/** Metrics values returned to Graphite */
	case class MetricsValues(val timeInfo: TimeInfo, val dataPoints: Seq[Option[Double]])
	
	/** Lists all metrics known to this store */
	def listMetrics(): List[String]
	
	/** Gets the metrics values for the given metric in the given time range */
	def getValues(metric: String, from: Date, until: Date): MetricsValues
	
	/** Cleans up resources used by this MetricsStore */
	def closeStore = { }

}
