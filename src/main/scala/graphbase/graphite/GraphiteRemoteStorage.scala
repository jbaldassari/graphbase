package graphbase.graphite

import graphbase.MetricsStore
import java.util.Date
import net.opentsdb.core.TSDB
import org.apache.log4j.Logger
import org.python.util.PythonInterpreter
import scala.annotation.tailrec
import scala.collection.immutable.Stream
import unfiltered.request._
import unfiltered.request.QParams._
import unfiltered.response._
import GraphiteRemoteStorage._

/** Mimics the Graphite Remote Storage API **/
trait GraphiteRemoteStorage extends unfiltered.filter.Plan with MetricsStore {
	val logger = Logger.getLogger(getClass())
	
	/** node.py */
	lazy val nodePy: String = {
		val is = classOf[GraphiteRemoteStorage].getResourceAsStream(NODE_PY_PATH)
		try {
			new String(Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray)
		} finally {
			is.close
		}
	}
	
	/** Logs an error, and generates an error response */
	def errorResponse(req: HttpRequest[Any], t: Throwable) = {
		val error = "Error handling " + req.uri + ": " + t
		logger.error(error, t)
		InternalServerError ~> PlainTextContent ~> ResponseString(error)
	}
	
	/** HTTP 400 + plaintext error message in response body */
	def badRequest(message: String) = 
		BadRequest ~> PlainTextContent ~> ResponseString(message)	
	
	/** HTTP 400 + plaintext error message about missing param in response body */
	def requiredParameterMissing(param: String) = 
		badRequest("Required parameter missing: " + param)

	/** HTTP 400 + plaintext error message about invalid param in response body */
	def invalidParameterValue(param: String) = 
		badRequest("Invalid value for parameter " + param)
	
	/** Parameters for the 'metrics/find' API call */
	case class FindParams(val query: String)
	
	/** Parser for FindParams */
	val findParamsParser = for {
		query <- lookup(QUERY_PARAM) is(required(requiredParameterMissing(QUERY_PARAM)))
	} yield FindParams(query.get)
	
	def nowInSeconds = System.currentTimeMillis / 1000
	def oneDayAgo = nowInSeconds - ONE_DAY_IN_SECONDS
	
	/** Parameters for the 'render' API call */
	case class RenderParams(val target: String, val from: Date, val until: Date)
	
	/** Parser for RenderParams */
	val renderParamsParser = for {
		target <- lookup(TARGET_PARAM) is(required(BadRequest ~> PlainTextContent ~> 
			ResponseString("Required parameter missing: " + TARGET_PARAM)))
		from <- lookup(FROM_PARAM) is(long(in => invalidParameterValue(FROM_PARAM)))
		until <- lookup(UNTIL_PARAM) is(long(in => invalidParameterValue(UNTIL_PARAM)))
	} yield RenderParams(target.get, new Date(from.getOrElse(oneDayAgo) * 1000L), 
			new Date(until.getOrElse(nowInSeconds) * 1000L))
	
	/** Represents a metrics node */
	case class Node(metricPath: String, isLeaf: Boolean)
	
	/** Handler for API calls */
	override def intent = {
		case req@GET(Path(Seg("metrics" :: "find" :: Nil)) & Params(params)) => 
			logger.info("Received request: " + req.uri)
			try {
				findParamsParser(params) match {
					case Left(fails) => fails.head.error
					case Right(findParams) => handleFindMetricsRequest(findParams)
				}
			} catch {
				case e: Exception => errorResponse(req, e)
			}
		case req@GET(Path(Seg("render" :: Nil)) & Params(params)) => 
			logger.info("Received request: " + req.uri)
			try {
				renderParamsParser(params) match {
					case Left(fails) => fails.head.error
					case Right(renderParams) => handleRenderRequest(renderParams)
				}
			} catch {
				case e: Exception => errorResponse(req, e)
			}
		case req => 
			logger.info("Received request: " + req.uri)
			BadRequest ~> PlainTextContent ~>
				ResponseString("Invalid Request: " + req.uri)
	}
	
	/** Handler for the find/metrics API call */
	def handleFindMetricsRequest(params: FindParams) = 
		Ok ~> PICKLE_CONTENT_TYPE ~> 
			ResponseString(pickleNodes(
					generateNodesAtDepth(findMetrics(params.query), getDepth(params.query))))
	
	/** 
	 * Finds all metrics matching the given query.
	 * A metric matches the query if the query is a prefix of the metric name.
	 */
	def findMetrics(query: String): List[String] = 
		for (metric <- listMetrics()
			if (metric.matches(".*" + query.replaceAll("\\*", ".*") + ".*"))
		) yield metric
	
	/**
	 * Gets the depth of the given metric.
	 * The depth is simply 1 + the number of '.' characters contained in the metric.
	 */
	def getDepth(metric: String) = 1 + metric.count { _.equals('.') }
		
	/** Generates all Graphite Nodes at the given depth for the given metrics */
	def generateNodesAtDepth(metrics: List[String], depth: Int): Set[Node] = 
		metrics.foldLeft(Set[Node]()) {
			(nodes, metric) => getNodeAtDepth(metric, depth) match {
				case None => nodes
				case Some(node) => nodes + node
			}
		}
	
	/**
	 * Gets the node at the given depth for the given metric.
	 * For example, given the metric "a.b.c" with depth 2 will return:
	 * Some(Node("a.b", false))
	 * @return Some(Node) if a node was found at the given depth, 
	 * or None if no node was found at that depth
	 */
	def getNodeAtDepth(metric: String, depth: Int): Option[Node] = {
		@tailrec
		def getNode(path: List[String], currentDepth: Int = 1): Option[Node] = {
			val isLeaf = currentDepth == path.length
			if (currentDepth == depth) {
				// Found a node at the correct depth
				Some(Node(path.take(currentDepth).mkString("."), isLeaf))
			}
			else if (isLeaf) {
				// We've reached the end of the path before reaching the desired depth
				None
			}
			else {
				// Current depth is less than total path depth, so recurse down
				getNode(path, currentDepth + 1)
			}
		}

		if (metric.isEmpty()) None
		else getNode(metric.split("\\.").toList)
	}
	
	/** Serializes the given nodes in Python pickle format */
	def pickleNodes(nodes: Iterable[Node]): String = {
		if (logger.isDebugEnabled) logger.debug("Pickling nodes: " + nodes)
		val buffer = new StringBuffer()
		buffer.append("\nnodes = []\n")
		for (node <- nodes) {
			buffer.append("nodes.append(Node('" + node.metricPath + "'," + { if (node.isLeaf) 1 else 0 } + "))\n")
		}
		buffer.append("pickledNodes = pickle.dumps([ { 'metric_path' : n.metric_path, 'isLeaf' : n.isLeaf } for n in nodes ])\n")
		val python = buffer.toString
		if (logger.isDebugEnabled) logger.debug(python)
		val interpreter = new PythonInterpreter()
		interpreter.exec(nodePy)
		interpreter.exec(python)
		interpreter.eval("pickledNodes").toString()
	}
	
	/** Handler for the render API call */
	def handleRenderRequest(params: RenderParams) = 
		Ok ~> ContentType("application/pickle") ~> 
			ResponseString(pickleMetricsValues(
					params, getValues(params.target, params.from, params.until)))

	/** Pickles the given MetricsValues for the render API call */
	def pickleMetricsValues(params: RenderParams, metricsValues: MetricsValues): String = {
		// Generate python:
		if (logger.isDebugEnabled) logger.debug("Pickling values: " + metricsValues)
		val buffer = new StringBuffer()
		buffer.append("result = [ { 'name' : '").append(params.target).append("'")
		buffer.append(", 'start' : ").append(metricsValues.timeInfo.start.getTime / 1000L)
		buffer.append(", 'end' : ").append(metricsValues.timeInfo.end.getTime / 1000L) 
		buffer.append(", 'step' : ").append(metricsValues.timeInfo.step) 
		buffer.append(", 'values' : [")
		buffer.append(metricsValues.dataPoints.foldLeft("") { 
			(values, point) => values + { point match { case Some(p) => p.toString case None => "None" } } + ","
		})
		buffer.append("] } ]\n")
		buffer.append("pickledValues = pickle.dumps(result)")
		val python = buffer.toString
		if (logger.isDebugEnabled) logger.debug(python)
		
		// Interpret python:
		val interpreter = new PythonInterpreter()
		interpreter.exec("import pickle")
		interpreter.exec(nodePy)
		interpreter.exec(python)
		interpreter.eval("pickledValues").toString()
	}
	
	override def destroy = closeStore
}

/** GraphiteRemoteStorage companion object */
object GraphiteRemoteStorage {
	val QUERY_PARAM = "query"
	val TARGET_PARAM = "target"
	val FROM_PARAM = "from"
	val UNTIL_PARAM = "until"
	val NODE_PY_PATH = "/node.py"
	val PICKLE_CONTENT_TYPE = ContentType("application/pickle")
	val ONE_DAY_IN_SECONDS = 24L * 60L * 60L
}
