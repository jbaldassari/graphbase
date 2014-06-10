package graphbase

import com.frugalmechanic.optparse._
import org.apache.log4j._

/** Main class for starting Graphbase */
object Graphbase extends OptParse {
	val DEFAULT_PORT = 8080
	val DFEAULT_METRICS_RELOAD_PERIOD_SECONDS = 60
	private[this] val logger = Logger.getLogger(classOf[OpenTsdbGraphiteRemoteStorage])
	val openTsdbConfig = StrOpt()
	val port = IntOpt()
	val step = IntOpt()
	val metricsReloadPeriod = IntOpt()
	
	/** Parses command-line arguments and starts Graphbase */
	def main(args: Array[String]): Unit = {
		parse(args)
		
		val openTsdbConfigPath: String = 
			if (openTsdbConfig) openTsdbConfig.get
			else {
				logger.fatal("openTsdbConfig not specified")
				System.exit(1)
				null
			}
		
		val graphiteStep = step.getOrElse(graphbase.opentsdb.OpenTsdbStore.DEFAULT_STEP)
		val reloadPeriod = metricsReloadPeriod.getOrElse(DFEAULT_METRICS_RELOAD_PERIOD_SECONDS)
		
		// Initialize/start embedded Jetty server
		val jettyPort = port.getOrElse(DEFAULT_PORT)
		val graphbaseFilter = OpenTsdbGraphiteRemoteStorage(openTsdbConfigPath, graphiteStep, reloadPeriod)
		val jetty = unfiltered.jetty.Http(jettyPort).filter(graphbaseFilter)
		jetty.start()
		
		logger.info("Graphbase running on port " + jettyPort)
	}
}
