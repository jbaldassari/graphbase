package graphbase

import graphbase.graphite.GraphiteRemoteStorage
import graphbase.opentsdb.OpenTsdbStore

/** Graphite remote storage API backed by OpenTSDB */
class OpenTsdbGraphiteRemoteStorage(
		override val configurationPath: String, 
		override val step: Long,
		override val metricsReloadPeriodSeconds: Int) extends GraphiteRemoteStorage with OpenTsdbStore {
	
}

/** Companion object for OpenTsdbGraphiteRemoteStorage */
object OpenTsdbGraphiteRemoteStorage {
	def apply(configurationPath: String, step: Long, metricsReloadPeriodSeconds: Int): OpenTsdbGraphiteRemoteStorage = 
		new OpenTsdbGraphiteRemoteStorage(configurationPath, step, metricsReloadPeriodSeconds)
}
