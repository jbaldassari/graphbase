package graphbase.opentsdb

import graphbase.MetricsStore
import java.util.Date
import java.util.Timer
import java.util.TimerTask
import net.opentsdb.core._
import net.opentsdb.tree._
import net.opentsdb.utils._
import org.apache.log4j.Logger
import org.hbase.async._
import scala.annotation.tailrec
import scala.collection.Seq
import scala.collection.immutable._
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.math._
import OpenTsdbStore._

/** A MetricsStore backed by OpenTSDB */
trait OpenTsdbStore extends MetricsStore {
	val configurationPath: String
	val step: Long = DEFAULT_STEP
	val tsdb: TSDB = new TSDB(new Config(configurationPath))
	val metricsReloadPeriodSeconds: Int
	@volatile var leafToId: SortedMap[String, String] = findMetrics()
	
	/** Container class for OpenTSDB data points */
	case class OpenTsdbDataPoint(val timestamp: Long, val value: Double)
	
	// Kick off a timer to periodically reload the metrics metadata from OpenTSDB:
	val timer: Timer = {
		val refreshMillis = metricsReloadPeriodSeconds * 1000L
		val timer = new Timer("Metrics Refresh Timer", /* daemon */ true)
		timer.schedule(new TimerTask() {
			override def run() = {
				leafToId = findMetrics()
			}
		}, refreshMillis, refreshMillis)
		timer
	}
	
	/** Walks all OpenTSDB trees and returns a map of fully-qualified metric name to TSUID */
	def findMetrics(): SortedMap[String, String] = {
		val startTime = System.nanoTime
		logger.info("Searching all OpenTSDB trees for metrics...")
		
		/** Enumerates all metrics in the given tree */
		def walkTree(tree: Tree): SortedMap[String, String] = {
			val branch = new Branch(tree.getTreeId())
			branch.prependParentPath(java.util.Collections.emptyMap())
			branch.setDisplayName(ROOT_BRANCH_NAME)
			val leaves = walkBranch(branch.getBranchId)
			if (logger.isDebugEnabled) logger.debug("Leaves for tree \"" + tree.getName() + "\": " + leaves)
			leaves
		}
		
		/** Meta-data used during tree traversal */
		case class BranchAndPath(val branchId: String, val path: String)
		
		/** Performs a depth-first search starting with the given branch and accumulating metric names as it goes **/
		@tailrec
		def walkBranch(branchId: String, next: List[BranchAndPath] = Nil, parentPath: String = "", leafIds: SortedMap[String, String] = TreeMap()): SortedMap[String, String] = {
			val branch = Branch.fetchBranch(tsdb, Branch.stringToId(branchId), true).join()
			val children = branch.getBranches
			val leaves = branch.getLeaves
			val pathAtDepth = {
				if (branch.getDisplayName.equals(ROOT_BRANCH_NAME)) ""
				else if (parentPath.isEmpty) branch.getDisplayName 
				else parentPath + "." + branch.getDisplayName
			}
			
			// Add metric/leaf data from the current tree depth:
			val leafIdsToDepth = 
				if (leaves == null) leafIds
				else leafIds ++ leaves.map(leaf => (pathAtDepth + "." + leaf.getDisplayName) -> leaf.getTsuid)
			
			if ((children == null) || children.isEmpty) {
				next match {
					case Nil => leafIdsToDepth
					case head :: tail => walkBranch(head.branchId, tail, head.path, leafIdsToDepth)
				}
			}
			else {
				val childBranches = children.tail.foldLeft(next) { (nextBranches, child) => BranchAndPath(child.getBranchId, pathAtDepth) :: nextBranches }
				walkBranch(children.head.getBranchId, childBranches, pathAtDepth, leafIdsToDepth)
			}
		}
		
		val trees = Tree.fetchAllTrees(tsdb).join()
		val metrics = trees.foldLeft(new TreeMap[String, String]()) {
			(metrics, tree) => metrics ++ walkTree(tree)
		}
		if (logger.isDebugEnabled) logger.debug("All metrics: " + metrics)
		
		val durationMillis = round((System.nanoTime - startTime) / 1e6d)
		logger.info("Found " + metrics.size + " OpenTSDB metrics in " + durationMillis + "ms.")
		metrics
	}
	
	/** Finds all metrics in all trees */
	override def listMetrics() = {
		if (logger.isDebugEnabled) logger.debug("Listing metrics: " + leafToId.keySet)
		leafToId.keySet.toList
	}
	
	override def getValues(metric: String, from: Date, until: Date): MetricsValues = {
		// Retrieve data if this metric is known:
		leafToId.get(metric) match {
			case Some(tsuid) => {
				if (logger.isDebugEnabled) logger.debug("Fetching values for metric " + metric + " with TSUID " + tsuid)
				fetchValues(tsuid, from, until)
			}
			case None => {
				logger.warn("No TSUID found for requested metric \"" + metric + "\"")
				MetricsValues(TimeInfo(from, until, step), Seq[Option[Double]]())
			}
		}
	}
	
	/** Retrieves metrics data from OpenTSDB and downsamples it to regularly spaced intervals */
	private def fetchValues(tsuid: String, from: Date, until: Date): MetricsValues = {
		val fetchStartTime = System.nanoTime
		val query = tsdb.newQuery()
		query.setStartTime(from.getTime)
		query.setEndTime(until.getTime)
		query.setTimeSeries(java.util.Arrays.asList(tsuid), Aggregators.SUM, false)
		
		val results = query.run()
		if (logger.isDebugEnabled) {
			logger.debug("Completed fetching values for TSUID " + tsuid + 
					" in " + round((System.nanoTime - fetchStartTime) / 1e6d) + "ms.")	
		}
		
		val values = if (results.length == 0) {
			MetricsValues(TimeInfo(from, until, step), Seq[Option[Double]]())
		}
		else {
			val raw = dataPoints2Seq(results(0), from)
			val points = downsample(from, until, raw)
			if (logger.isDebugEnabled) logger.debug("Downsampled points (" + points.size + "): " + points)
			MetricsValues(TimeInfo(from, until, step), points)
		}
		
		if (logger.isDebugEnabled) {
			logger.debug("Completed processing values for TSUID " + tsuid + 
					" in " + round((System.nanoTime - fetchStartTime) / 1e6d) + "ms.")	
		}
		
		values
	}
	
	/** Converts an OpenTSDB DataPoints instance to a sequence of OpenTsdbDataPoint instances */
	def dataPoints2Seq(dataPoints: DataPoints, from: Date): Seq[OpenTsdbDataPoint] = {
		@tailrec
		def dataPointIter2SeqTR(iter: Iterator[DataPoint], points: ListBuffer[OpenTsdbDataPoint] = ListBuffer()): Seq[OpenTsdbDataPoint] = {
			if (!iter.hasNext) points.toSeq
			else {
				val next = iter.next
				dataPointIter2SeqTR(iter, points += OpenTsdbDataPoint(next.timestamp, next.toDouble))
			}
		}
		val iter = {
			val dpi = dataPoints.iterator
			dpi.seek(from.getTime)
			dpi
		}
		dataPointIter2SeqTR(iter)
	}
	
	/** Normalizes a sequence of data points with arbitrary timestamps into a sequence of regularly-spaced average values. */
	def downsample(from: Date, until: Date, dataPoints: Seq[OpenTsdbDataPoint]): Seq[Option[Double]] = {
		val durationMillis = until.getTime - from.getTime
		val stepCount = round((durationMillis / 1000L) / step)
		val stepLengthMillis = durationMillis / stepCount
		if (logger.isTraceEnabled) {
			logger.trace("downsample - durationMillis: " + durationMillis)
			logger.trace("downsample - stepCount: " + stepCount)
			logger.trace("downsample - stepLengthMillis: " + stepLengthMillis)
		}
		
		@tailrec
		def downsampleTR(remainingPoints: Seq[OpenTsdbDataPoint], currentStep: Int = 0, currentStepPoints: Seq[Double] = Seq(), allStepPoints: Seq[Option[Double]] = Seq()): Seq[Option[Double]] = {
			if (logger.isTraceEnabled) {
				logger.trace("downsampleTR - remainingPoints: " + remainingPoints.size)
				logger.trace("downsampleTR - currentStep = " + currentStep)
				logger.trace("downsampleTR - currentStepPoints: " + currentStepPoints.size)
				logger.trace("downsampleTR - allStepPoints: " + allStepPoints.size)
			}
			if (currentStep == stepCount) {
				allStepPoints ++ Seq.fill(stepCount - currentStep) { None }
			}
			else {
				remainingPoints match {
					case Nil => allStepPoints ++ Seq.fill(stepCount - currentStep) { None }
					case head :: tail => {
						val stepOffset = currentStep * stepLengthMillis
						val stepStart = from.getTime + stepOffset
						val stepEnd = stepStart + stepLengthMillis
						val pointTimestamp = head.timestamp
						if (pointTimestamp < stepStart) {
							downsampleTR(tail, currentStep, currentStepPoints, allStepPoints)
						}
						else if (pointTimestamp < stepEnd) {
							downsampleTR(tail, currentStep, currentStepPoints :+ head.value, allStepPoints)
						}
						else {
							val avg: Option[Double] = if (currentStepPoints.isEmpty) None else Some(currentStepPoints.reduceLeft(_ + _) / currentStepPoints.size.toDouble)
							if (logger.isTraceEnabled) logger.trace("Step " + currentStep + " has " + currentStepPoints.size + " data points with average: " + avg)
							downsampleTR(head :: tail, currentStep + 1, List(), allStepPoints :+ avg)
						}
					}
				}
			}
		}
		
		downsampleTR(dataPoints)
	}
		
	override def closeStore() = {
		tsdb.shutdown()
		timer.cancel()
	}
}

/** Companion object for OpenTsdbStore */
object OpenTsdbStore {
	val DEFAULT_STEP = 60
	val ROOT_BRANCH_NAME = "ROOT"
	private[OpenTsdbStore] val logger = Logger.getLogger(getClass())
}
