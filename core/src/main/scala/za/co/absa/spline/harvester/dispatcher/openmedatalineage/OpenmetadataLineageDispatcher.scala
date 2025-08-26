
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
import com.google.gson.{JsonElement, JsonObject, JsonArray, JsonParser, Gson}
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher
import java.net.URI
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



  override protected def send(data: String): Unit = {
    if (data.startsWith("ExecutionPlan")) {
      val jsonData = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
      //创建pipeline Service  注意这里的id之后放置列级别的血缘会用到
       createOrUpdatePipelineService()
      //从这个jsondata中解析出sourceentity,targetentity,sourcetable,targettable 构建血缘
      sendMetadataLineage(jsonData)

    }
  }


  private def getEntity(serviceName: String,databaseName: String,tableName: String): Map[String, JsonObject] = {
    Try {
      val request = createGetTableRequest(Some(serviceName),databaseName,tableName)
      val response = sendSearchRequest(request)
      println(s"Response keys: ${response.keys}")
      val hitsResult = response("hits").asInstanceOf[Map[String, Any]]
      val totalHits = hitsResult("total").asInstanceOf[Map[String, Any]]("value").toString.toInt

      if (totalHits == 0) {
        println(s"Failed to get id of table from OpenMetadata.")
        Map.empty[String, JsonObject]
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
          val entityJson = new JsonObject()
          entityJson.addProperty("id", tableId)
          entityJson.addProperty("name", tableName)
          entityJson.addProperty("fullyQualifiedName", fullyQualifiedName)
          entityJson.addProperty("deleted", deleted)
          entityJson.addProperty("description", description)
          entityJson.addProperty("displayName", displayName)
          entityJson.addProperty("href", s"${config.hostPort}/api/v1/tables/$tableId")
          entityJson.addProperty("inherited", true)
          entityJson.addProperty("type", "table")
          tableName -> entityJson
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
      case Some(service) => s"$service.*$databaseName.*$tableName"
      case None => s"*$tableName"
    }
    createESRequest(fqnQuery, TABLE_SEARCH_INDEX)
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

    def createOrUpdatePipeline(): String = {
    try {
      val request = createPipelineRequest()
      val response = sendRequest(request)
      response.get("id") match {
        case Some(id) => id.toString
        case None => ""
      }
    } catch {
      case e: Exception =>
        log.error(s"Failed to create/update pipeline ${config.pipelineName} in OpenMetadata: ", e)
        throw new OpenLineageClientException(e)
    }
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

  def createPipelineRequest(): HttpRequest = {
    val requestMap = new util.HashMap[String, AnyRef]()
    requestMap.put("name", config.pipelineName)
    requestMap.put("sourceUrl", config.pipelineSourceUrl)
    if (config.pipelineDescription != null && config.pipelineDescription.nonEmpty) {
      requestMap.put("description", config.pipelineDescription)
    }
    requestMap.put("service", config.pipelineServiceName)
    val jsonRequest = toJsonString(requestMap)
    createPutRequest("/api/v1/pipelines", jsonRequest)
  }

  def toJsonString(obj: AnyRef): String = {
    val gson = new Gson()
    gson.toJson(obj)
  }

  def createPutRequest(path: String, jsonRequest: String): HttpRequest = {
      val fullUrl = s"${config.hostPort}$path"
      Http(fullUrl).put(jsonRequest).header("Content-Type", "application/json").header("Authorization", s"${config.jwtToken}")
  }

  private def sendRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.asString
      if (response.isSuccess) {
        val jsonResponse = JsonParser.parseString(response.body).getAsJsonObject
        Map("id" -> jsonResponse.get("id").getAsString)
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

  private def sendSearchRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.asString
      if (response.isSuccess) {
        val jsonResponse = JsonParser.parseString(response.body).getAsJsonObject
        convertJsonObjectToMap(jsonResponse)
      } else {
        throw new RuntimeException(s"HTTP search request failed with status: ${response.code}")
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        log.error(s"Failed to send search HTTP request: ${exception.getMessage}")
        throw exception
    }
  }


  def sendMetadataLineage(jsonData: String): Unit = {
    try {
      val piplinen_id = createOrUpdatePipeline()
      logDebug(s"原始血缘数据: $jsonData")
      val operations = JsonParser.parseString(jsonData).getAsJsonObject.get("operations").getAsJsonObject
      val write = operations.get("write").getAsJsonObject
      val targetType = getStringValue(write, "extra.destinationType")
      val targetDatabase = getStringValue(write, "params.table.identifier.database")
      val targetTableName = getStringValue(write, "params.table.identifier.table")
      val targetEntity: Map[String, JsonObject] = getEntity(targetType, targetDatabase, targetTableName)

      val readsArray = operations.get("reads").getAsJsonArray

      for (i <- 0 until readsArray.size()) {
        val readObj = readsArray.get(i).getAsJsonObject
        val sourceType = getStringValue(readObj, "extra.sourceType")
        val sourceTable = getStringValue(readObj, "params.table.identifier.table")
        val sourceDatabase = getStringValue(readObj, "params.table.identifier.database")
        logInfo(s"源${i + 1}信息 - 类型: '$sourceType', 数据库: '$sourceDatabase', 表: '$sourceTable'")
        val sourceEntity: Map[String, JsonObject] = getEntity(sourceType, sourceDatabase, sourceTable)

        if (sourceEntity.nonEmpty && targetEntity.nonEmpty) {
          val lineageRequest = createLineageRequest(piplinen_id, sourceEntity, targetEntity, jsonData, i)

          try {
            val response = sendRequest(lineageRequest)
            logInfo(s"Successfully created lineage from $sourceTable to $targetTableName")
          } catch {
            case e: Exception =>
              logError(s"Failed to create lineage from $sourceTable to $targetTableName: ${e.getMessage}")
          }
        } else {
          logWarning(s"Skipping lineage creation: sourceEntity.isEmpty=${sourceEntity.isEmpty}, targetEntity.isEmpty=${targetEntity.isEmpty}")
        }
      }
    } catch {
      case e: Exception =>
        logError(s"解析血缘数据时发生异常: ${e.getMessage}")
    }
  }


  def createLineageRequest(pipenameId: String, fromEntity: Map[String, JsonObject], toEntity: Map[String, JsonObject], jsonData: String, sourceIndex: Int): HttpRequest = {
    val fromEntityJson = fromEntity.values.head
    val toEntityJson = toEntity.values.head

    val edgeJson = new JsonObject()
    edgeJson.add("toEntity", toEntityJson)
    edgeJson.add("fromEntity", fromEntityJson)
    val lineageDetailsJson = new JsonObject()
    lineageDetailsJson.add("pipeline", createPipelineEntityJson(pipenameId))
    lineageDetailsJson.addProperty("source", SPARK_LINEAGE_SOURCE)

    val columnsLineageArray = new JsonArray()
    val columnLineageList = getColumnLevelLineage(jsonData, fromEntityJson.get("fullyQualifiedName").getAsString, toEntityJson.get("fullyQualifiedName").getAsString, sourceIndex)

    for (columnLineage <- columnLineageList) {
      val columnLineageJson = new JsonObject()
      val fromColumnsArray = new JsonArray()
      val fromColumns = columnLineage("fromColumns").asInstanceOf[List[String]]
      for (fromColumn <- fromColumns) {
        fromColumnsArray.add(fromColumn)
      }
      columnLineageJson.add("fromColumns", fromColumnsArray)
      columnLineageJson.addProperty("toColumn", columnLineage("toColumn").asInstanceOf[String])
      columnsLineageArray.add(columnLineageJson)
    }

    lineageDetailsJson.add("columnsLineage", columnsLineageArray)
    edgeJson.add("lineageDetails", lineageDetailsJson)

    val requestJson = new JsonObject()
    requestJson.add("edge", edgeJson)

    val jsonRequest = toJsonString(requestJson)
    createPutRequest("/api/v1/lineage", jsonRequest)
  }

  private def createPipelineEntityJson(pipenameId: String): JsonObject = {
    val pipelineJson = new JsonObject()
    pipelineJson.addProperty("id", pipenameId)
    pipelineJson.addProperty("type", "pipeline")
    pipelineJson
  }



  private def convertJsonObjectToMap(jsonObject: JsonObject): Map[String, Any] = {
    val result = scala.collection.mutable.Map[String, Any]()
    val entrySet = jsonObject.entrySet()
    import scala.collection.JavaConverters._
    for (entry <- entrySet.asScala) {
      val key = entry.getKey
      val value = entry.getValue
      if (value.isJsonPrimitive) {
        val primitive = value.getAsJsonPrimitive
        if (primitive.isBoolean) {
          result(key) = primitive.getAsBoolean
        } else if (primitive.isNumber) {
          result(key) = primitive.getAsNumber
        } else {
          result(key) = primitive.getAsString
        }
      } else if (value.isJsonObject) {
        // 递归处理嵌套的JSON对象
        result(key) = convertJsonObjectToMap(value.getAsJsonObject)
      } else if (value.isJsonArray) {
        // 处理JSON数组
        val array = value.getAsJsonArray
        val list = scala.collection.mutable.ListBuffer[Any]()
        for (i <- 0 until array.size()) {
          val element = array.get(i)
          if (element.isJsonPrimitive) {
            val primitive = element.getAsJsonPrimitive
            if (primitive.isBoolean) {
              list += primitive.getAsBoolean
            } else if (primitive.isNumber) {
              list += primitive.getAsNumber
            } else {
              list += primitive.getAsString
            }
          } else if (element.isJsonObject) {
            list += convertJsonObjectToMap(element.getAsJsonObject)
          } else {
            list += element.toString
          }
        }
        result(key) = list.toList
      } else {
        result(key) = value.toString
      }
    }
    result.toMap
  }

  private def getColumnLevelLineage(jsonData: String, sourceTableFqn: String, targetTableFqn: String, sourceIndex: Int): List[Map[String, Any]] = {
    try {
      val json = JsonParser.parseString(jsonData).getAsJsonObject

      val attributesMap = createAttributesMap(json)
      val op1OutputList = getOp1OutputListWithNames(json, attributesMap)
      val readsOutputInfo = getReadsOutputInfoWithNames(json, attributesMap)
      val filteredFunctions = getFilteredFunctionsWithNames(json, attributesMap)
      
      // 生成列级别血缘关系，使用传入的sourceTableFqn和targetTableFqn
      val columnLineageJson = generateColumnLineage(json, attributesMap, op1OutputList, readsOutputInfo, filteredFunctions, sourceTableFqn, targetTableFqn)
      
      // 转换为OpenmetadataLineageDispatcher需要的格式，并确保包含TableFqn前缀
      val lineageResults = mutable.ListBuffer[Map[String, Any]]()
      
      for (i <- 0 until columnLineageJson.size()) {
        val lineageObj = columnLineageJson.get(i).getAsJsonObject
        val fromColumnsArray = lineageObj.get("fromColumns").getAsJsonArray
        val toColumn = lineageObj.get("toColumn").getAsString
        
        val fromColumns = (0 until fromColumnsArray.size()).map { j =>
          fromColumnsArray.get(j).getAsString
        }.toList
        
        lineageResults += Map(
          "fromColumns" -> fromColumns,
          "toColumn" -> toColumn
        )
      }
      
      logInfo(s"Generated ${lineageResults.size} column lineage entries for source table index $sourceIndex")
      lineageResults.toList
    } catch {
      case e: Exception =>
        logError(s"Failed to parse column level lineage: ${e.getMessage}")
        List.empty[Map[String, Any]]
    }
  }



  private def getStringValue(json: JsonObject, path: String): String = {
    try {
      val keys = path.split("\\.")
      val lastIndex = keys.length - 1
      val parentObj = keys.dropRight(1).foldLeft(json) { (obj, key) =>
        if (obj != null && obj.has(key)) {
          obj.get(key).getAsJsonObject
        } else {
          null
        }
      }
      if (parentObj != null && parentObj.has(keys(lastIndex))) {
        val value = parentObj.get(keys(lastIndex)).getAsString
        value
      } else {
        ""
      }
    } catch {
      case e: Exception =>
        ""
    }
  }


  private def createAttributesMap(jsonObject: JsonObject): mutable.Map[String, String] = {
    val attributesMap = mutable.Map[String, String]()

    if (jsonObject.has("attributes")) {
      val attributesArray = jsonObject.get("attributes").getAsJsonArray()

      for (i <- 0 until attributesArray.size()) {
        val attrObj = attributesArray.get(i).getAsJsonObject()
        attributesMap(attrObj.get("id").getAsString()) = attrObj.get("name").getAsString()
      }
    }

    attributesMap
  }


  private def getOp1OutputListWithNames(jsonObject: JsonObject, attributesMap: mutable.Map[String, String]): List[String] = {
    if (jsonObject.has("operations")) {
      val operationsObj = jsonObject.get("operations").getAsJsonObject()

      if (operationsObj.has("other")) {
        val otherArray = operationsObj.get("other").getAsJsonArray()

        for (i <- 0 until otherArray.size()) {
          val operationObj = otherArray.get(i).getAsJsonObject()
          if ("op-1".equals(operationObj.get("id").getAsString()) && operationObj.has("output")) {
            val outputArray = operationObj.get("output").getAsJsonArray()
            return (0 until outputArray.size()).map(j =>
              attributesMap.getOrElse(outputArray.get(j).getAsString(), outputArray.get(j).getAsString())
            ).toList
          }
        }
      }
    }

    List.empty[String]
  }

  private def getReadsOutputInfoWithNames(jsonObject: JsonObject, attributesMap: mutable.Map[String, String]): JsonArray = {
    val readsArray = new JsonArray()
    val uniqueEntries = mutable.Set[String]()

    if (jsonObject.has("operations")) {
      val operationsObj = jsonObject.get("operations").getAsJsonObject()

      if (operationsObj.has("reads")) {
        val readsList = operationsObj.get("reads").getAsJsonArray()

        for (i <- 0 until readsList.size()) {
          val readObj = readsList.get(i).getAsJsonObject()
          val readInfo = new JsonObject()

          // 提取数据库名和表名
          if (readObj.has("params") && readObj.get("params").getAsJsonObject().has("table")) {
            val tableObj = readObj.get("params").getAsJsonObject().get("table").getAsJsonObject()
            if (tableObj.has("identifier")) {
              val identifierObj = tableObj.get("identifier").getAsJsonObject()
              val database = identifierObj.get("database").getAsString()
              val table = identifierObj.get("table").getAsString()
              readInfo.addProperty("database", database)
              readInfo.addProperty("table", table)

              // 提取output列表并将列ID转换为列名
              if (readObj.has("output")) {
                val outputArray = readObj.get("output").getAsJsonArray()
                val outputNamesList = (0 until outputArray.size()).map(j =>
                  attributesMap.getOrElse(outputArray.get(j).getAsString(), outputArray.get(j).getAsString())
                ).toList.sorted

                val uniqueKey = database + "|" + table + "|" + outputNamesList.mkString(",")

                if (!uniqueEntries.contains(uniqueKey)) {
                  uniqueEntries.add(uniqueKey)

                  val outputNamesArray = new JsonArray()
                  outputNamesList.foreach(outputNamesArray.add)
                  readInfo.add("output", outputNamesArray)

                  readsArray.add(readInfo)
                }
              }
            }
          }
        }
      }
    }

    readsArray
  }

  private def getFilteredFunctionsWithNames(jsonObject: JsonObject, attributesMap: mutable.Map[String, String]): JsonArray = {
    val filteredArray = new JsonArray()
    val uniqueEntries = mutable.Set[String]()

    Option(jsonObject.getAsJsonObject("expressions"))
      .flatMap(expr => Option(expr.getAsJsonArray("functions")))
      .getOrElse(new JsonArray())
      .iterator()
      .asScala
      .map(_.getAsJsonObject)
      .filter { func =>
        val hasAttrIdWithAttr = Option(func.getAsJsonArray("childRefs"))
          .exists(arr => arr.iterator().asScala.exists(elem =>
            elem.getAsJsonObject.has("__attrId") &&
              elem.getAsJsonObject.get("__attrId").getAsString.contains("attr")
          ))

        val hasAliasTypeHint = Option(func.getAsJsonObject("extra"))
          .exists(extra => "expr.Alias".equals(extra.get("_typeHint").getAsString))

        hasAttrIdWithAttr && hasAliasTypeHint
      }
      .foreach { func =>
        val paramsName = Option(func.getAsJsonObject("params"))
          .map(params => params.get("name").getAsString)
          .getOrElse("")

        val columnNames = Option(func.getAsJsonArray("childRefs"))
          .getOrElse(new JsonArray())
          .iterator()
          .asScala
          .map(_.getAsJsonObject)
          .filter(_.has("__attrId"))
          .map(_.get("__attrId").getAsString)
          .filter(_.contains("attr"))
          .map(attrId => attributesMap.getOrElse(attrId, attrId))
          .toList
          .sorted

        val uniqueKey = paramsName + "|" + columnNames.mkString(",")

        if (!uniqueEntries.contains(uniqueKey)) {
          uniqueEntries.add(uniqueKey)

          val resultObj = new JsonObject()
          resultObj.addProperty("params_name", paramsName)

          val columnNamesArray = new JsonArray()
          columnNames.foreach(columnNamesArray.add)
          resultObj.add("column_names", columnNamesArray)

          filteredArray.add(resultObj)
        }
      }

    filteredArray
  }


  private def generateColumnLineage(jsonObject: JsonObject, attributesMap: mutable.Map[String, String],
    writeColumns: List[String], readTablesInfo: JsonArray,
    columnMappings: JsonArray, sourceTableFqn: String, targetTableFqn: String): JsonArray = {
    
    // 使用传入的targetTableFqn作为目标表
    if (targetTableFqn.isEmpty) return new JsonArray()

    // 使用传入的sourceTableFqn作为源表
    if (sourceTableFqn.isEmpty) return new JsonArray()

    val readTableColumns = (0 until readTablesInfo.size()).iterator
      .map(readTablesInfo.get(_).getAsJsonObject)
      .map(obj => (obj.get("database").getAsString + "." + obj.get("table").getAsString,
        obj.get("output").getAsJsonArray.iterator().asScala.map(_.getAsString).toList))
      .toMap

    val columnMapping = (0 until columnMappings.size()).iterator
      .map(columnMappings.get(_).getAsJsonObject)
      .flatMap(obj => Option(obj.getAsJsonArray("column_names"))
        .filter(_.size() > 0)
        .map(arr => obj.get("params_name").getAsString -> arr.get(0).getAsString))
      .toMap

    val resultArray = new JsonArray()

    writeColumns.foreach { writeCol =>
      // 使用传入的targetTableFqn构建toColumn
      val target = s"$targetTableFqn.$writeCol"
      val sourceCol = columnMapping.getOrElse(writeCol, writeCol)

      readTableColumns.foreach { case (table, cols) =>
        if (cols.contains(sourceCol)) {
          val lineageObj = new JsonObject()
          val fromColumnsArray = new JsonArray()
          // 使用传入的sourceTableFqn构建fromColumns
          fromColumnsArray.add(s"$sourceTableFqn.$sourceCol")

          lineageObj.add("fromColumns", fromColumnsArray)
          lineageObj.addProperty("toColumn", target)
          resultArray.add(lineageObj)
        }
      }
    }

    resultArray
  }


}
