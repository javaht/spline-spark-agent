
/*
 * Copyright 2021 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package za.co.absa.spline.harvester.dispatcher.openmedatalineage
import org.apache.commons.configuration.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import scalaj.http.{Http, HttpRequest}

import scala.util.{Failure, Success, Try}
import scala.collection.mutable
import com.alibaba.fastjson2.{JSON, JSONObject}
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher

import java.net.URI
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import java.util

class OpenLineageClientException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(cause: Throwable) = this(cause.getMessage, cause)
  def this(message: String) = this(message, null)
}

class OpenmetadataLineageDispatcher(

  val config: OpenmetadataLineageDispatcherConfig) extends AbstractJsonLineageDispatcher with Logging {

  def this(configuration: Configuration) = this(new OpenmetadataLineageDispatcherConfig(configuration))

  // 常量定义
  private val SPARK_LINEAGE_SOURCE: String = "SparkLineage"
  private val TABLE_SEARCH_INDEX: String = "table_search_index"
  private val PIPELINE_SOURCE_TYPE: String = "Spark"

  private var databasenames: List[String] = List.empty[String]
  override def name = "Openmetadata"

  if (config.databaseServiceNames != null) {
    try {
      databasenames=config.databaseServiceNames.split(",").toList
    } catch {
      case e: Exception =>
        log.error("failed to emit fetch database service names: {}", e.getMessage, e)
        List.empty[String]
    }
  } else {
    databasenames= List.empty[String]
  }
  //第一步做这个
  createOrUpdatePipelineService()


  override protected def send(data: String): Unit = {
    if (data.startsWith("ExecutionPlan")) {
      val jsonData = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
      //从这个jsondata中解析出sourceentity,targetentity,sourcetable,targettable
      val (sourceset, target) = getSourceAndTarget(jsonData)

    }
  }


  private def getEntity(serviceName: String,databaseName: String,tableName: String): Map[String, JSONObject] = {
    Try {
      val request = createGetTableRequest(Some(serviceName),databaseName,tableName)
      val response = sendRequest(request)
      println(s"Response keys: ${response.keys}")
      val hitsResult = response("hits").asInstanceOf[Map[String, Any]]
      val totalHits = hitsResult("total").asInstanceOf[Map[String, Any]]("value").toString.toInt

      if (totalHits == 0) {
        println(s"Failed to get id of table from OpenMetadata.")
        Map.empty[String, JSONObject]
      } else {
        val tablesData = hitsResult("hits").asInstanceOf[List[Map[String, Any]]]
        println(s"Found ${tablesData.length} tables")
        val resultMap = tablesData.map { tableHit =>
          val tableSource = tableHit("_source").asInstanceOf[Map[String, Any]]
          val tableName = tableSource.getOrElse("name", "unknown").toString
          val tableId = tableSource.getOrElse("id", "").toString
          val fullyQualifiedName = tableSource.getOrElse("fullyQualifiedName", "").toString
          val description = tableSource.getOrElse("description", "").toString
          val displayName = tableSource.getOrElse("displayName", tableName).toString
          val deleted = tableSource.getOrElse("deleted", false).asInstanceOf[Boolean]
          val fromEntityJson = new JSONObject()
          fromEntityJson.put("id", tableId: Object)
          fromEntityJson.put("name", tableName: Object)
          fromEntityJson.put("fullyQualifiedName", fullyQualifiedName: Object)
          fromEntityJson.put("deleted", java.lang.Boolean.valueOf(deleted): Object)
          fromEntityJson.put("description", description: Object)
          fromEntityJson.put("displayName", displayName: Object)
          fromEntityJson.put("href", s"${config.hostPort}/api/v1/tables/$tableId": Object)
          fromEntityJson.put("inherited", java.lang.Boolean.valueOf(true): Object)
          fromEntityJson.put("type", "table": Object)
          tableName -> fromEntityJson
        }.toMap
        resultMap
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"Failed to get table entity from OpenMetadata: $exception")
        throw new Exception(exception)
    }
  }



  def createGetTableRequest(dbServiceName: Option[String] = None,databaseName: String,tableName: String): HttpRequest = {
    val fqnQuery = dbServiceName match {
      case Some(service) => s"$service.*.$databaseName.*$tableName"
      case None => s"*$tableName"
    }
    createESRequest( fqnQuery, TABLE_SEARCH_INDEX)
  }

    def createESRequest(fieldValue: String, index: String): HttpRequest = {
      val path = "api/v1/search/fieldQuery"
      val queryParams = Map(
        "size" -> "10",
        "fieldName" -> "fullyQualifiedName",
        "fieldValue" -> fieldValue,
        "from" -> "0",
        "index" -> index
      )
      createHttpRequest(path, queryParams)
    }

    private def createHttpRequest(path: String, queryParams: Map[String, String]): HttpRequest = {
      val baseUri = new URI(config.hostPort)
      val fullUrl = s"${baseUri.getScheme}://${baseUri.getHost}:${baseUri.getPort}/$path"
      var request = Http(fullUrl).params(queryParams).header("Accept", "application/json").header("Content-Type", "application/json")
      request = request.header("Authorization",  config.jwtToken)
      request
    }

  private def createOrUpdatePipeline(): String = {
    try {
      val request = createPipelineRequest()
      val response = sendRequest(request)
      response.get("id").toString
    } catch {
      case e: Exception =>
        log.error(s"Failed to create/update pipeline ${config.pipelineName} in OpenMetadata: ", e)
        throw new OpenLineageClientException(e)
    }
  }


  def createPipelineRequest(): HttpRequest = {
    val requestMap = scala.collection.mutable.Map[String, Object]()
    requestMap.put("name", ${config.pipelineName})
    requestMap.put("sourceUrl", ${config.pipelineSourceUrl})

    if (${config.pipelineDescription} != null && ${config.pipelineDescription} .nonEmpty) {
      requestMap.put("description", ${config.pipelineDescription} )
    }

    requestMap.put("service", ${config.serviceNames} )
    val jsonRequest = toJsonString(requestMap)
    createPutRequest("/api/v1/pipelines", jsonRequest)
  }



  private def createOrUpdatePipelineService(): String = {
    try {
      val request = createPipelineServiceRequest()
      val response = sendRequest(request)
      response.get("id").toString
    } catch {
      case e: Exception =>
        log.error(s"Failed to create/update service pipeline ${config.pipelineServiceName} in OpenMetadata: ", e)
        throw new OpenLineageClientException(e)
    }
  }


  def createPipelineServiceRequest(): HttpRequest = {
      val requestMap = new util.HashMap[String, AnyRef]
      requestMap.put("name", config.pipelineServiceName)
      requestMap.put("serviceType", PIPELINE_SOURCE_TYPE)
      val connectionConfig = new util.HashMap[String, AnyRef]
      val connectionType = new util.HashMap[String, AnyRef]
      connectionType.put("type", PIPELINE_SOURCE_TYPE)
      connectionConfig.put("config", connectionType)
      requestMap.put("connection", connectionConfig)
      val jsonRequest = toJsonString(requestMap)
      createPutRequest("/api/v1/services/pipelineServices", jsonRequest)
  }

  def toJsonString(obj: AnyRef): String = JSON.toJSONString(obj)

  def createPutRequest(path: String, jsonRequest: String): HttpRequest = {
      val fullUrl = s"${config.hostPort}$path"
      Http(fullUrl).put(jsonRequest).header("Content-Type", "application/json")
  }

  private def sendRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.header("Authorization", s"Bearer ${config.jwtToken}").asString
      if (response.isSuccess) {
        val jsonResponse = JSON.parseObject(response.body)
        Map("id" -> jsonResponse.getString("id"))
      } else {
        throw new RuntimeException(s"HTTP request failed with status: ${response.code}")
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        log.error(s"Failed to send HTTP request: ${exception.getMessage}")
        throw exception
    }
  }




  def getSourceAndTarget(jsonData: String): (mutable.LinkedHashSet[(String, String, String)], (String, String, String)) = {
    try {
      logDebug(s"原始血缘数据: $jsonData")
      val pipelineId = createOrUpdatePipeline()
      val operations = JSON.parseObject(jsonData).getJSONObject("operations")
      val write = operations.getJSONObject("write")
      val targetType = getStringValue(write, "extra.destinationType")
      val targetDatabase = getStringValue(write, "params.table.identifier.database")
      val targetTableName = getStringValue(write, "params.table.identifier.table")
      val targetEntity: Map[String, JSONObject] = getEntity(targetType, targetDatabase, targetTableName)

      val readsArray = operations.getJSONArray("reads")
      val sourceset = mutable.LinkedHashSet[(String, String, String)]()

      for (i <- 0 until readsArray.size()) {
        val readObj = readsArray.getJSONObject(i)
        val sourceType = getStringValue(readObj, "extra.sourceType")
        val sourceTable = getStringValue(readObj, "params.table.identifier.table")
        val sourceDatabase = getStringValue(readObj, "params.table.identifier.database")
        logInfo(s"源${i + 1}信息 - 类型: '$sourceType', 数据库: '$sourceDatabase', 表: '$sourceTable'")
        val sourceEntity = getEntity(sourceType, sourceDatabase, sourceTable)
        createLineageRequest(pipelineId, sourceEntity, targetEntity, sourceTable, targetTableName)
      }





      val target = (targetTableName, "", "")
      (sourceset, target)
    } catch {
      case e: Exception =>
        logError(s"解析血缘数据时发生异常: ${e.getMessage}")
        (mutable.LinkedHashSet[(String, String, String)](), ("", "", ""))
    }
  }





  def createLineageRequest(
    pipelineId: String,
    fromEntity: Map[String, Any],
    toEntity: Map[String, Any],
    fromTable: String,
    toTable: String
  ): HttpRequest = {

    val edgeMap = Map(
      "toEntity" -> createEntityMap(toEntity("entityType").toString, toEntity("id").toString),
      "fromEntity" -> createEntityMap(fromEntity("entityType").toString, fromEntity("id").toString)
    )

    val lineageDetails = Map(
      "pipeline" -> createEntityMap("pipeline", pipelineId),
      "source" -> SPARK_LINEAGE_SOURCE,
      "columnsLineage" -> getColumnLevelLineage(fromEntity, toEntity, fromTable, toTable)
    )

    val edgeMapWithLineage = edgeMap + ("lineageDetails" -> lineageDetails)

    val requestMap = Map("edge" -> edgeMapWithLineage)

    val jsonRequest = toJsonString(requestMap)

    Http("/api/v1/lineage")
      .method("PUT")
      .postData(jsonRequest)
      .header("Content-Type", "application/json")
  }

  private def createEntityMap(entityType: String, id: String): Map[String, Any] = {
    Map("type" -> entityType, "id" -> id)
  }

  private def getStringValue(json: JSONObject, path: String): String = {
    try {
      val keys = path.split("\\.")
      val lastIndex = keys.length - 1
      val parentObj = keys.dropRight(1).foldLeft(json) { (obj, key) =>
        if (obj != null && obj.containsKey(key)) {
          obj.getJSONObject(key)
        } else {
          logDebug(s"路径 '$path' 中的键 '$key' 不存在或为null")
          null
        }
      }
      if (parentObj != null && parentObj.containsKey(keys(lastIndex))) {
        val value = parentObj.getString(keys(lastIndex))
        logDebug(s"成功提取路径 '$path' 的值: '$value'")
        value
      } else {
        logDebug(s"路径 '$path' 的最终键 '${keys(lastIndex)}' 不存在")
        ""
      }
    } catch {
      case e: Exception =>
        logWarning(s"提取路径 '$path' 的值时发生异常: ${e.getMessage}")
        ""
    }
  }


}
