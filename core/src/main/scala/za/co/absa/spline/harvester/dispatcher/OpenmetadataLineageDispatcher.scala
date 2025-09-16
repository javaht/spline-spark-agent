package za.co.absa.spline.harvester.dispatcher
import org.apache.commons.configuration.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import scalaj.http.{Http, HttpRequest}
import scala.util.{Failure, Success, Try}
import scala.collection.mutable
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import za.co.absa.spline.harvester.dispatcher.openmedatalineage.OpenmetadataLineageDispatcherConfig
import java.net.URI
import scala.collection.JavaConverters._
import java.util

class OpenLineageClientException(message: String, cause: Throwable) extends Exception(message, cause) {
  def this(cause: Throwable) = this(cause.getMessage, cause)
  def this(message: String) = this(message, null)
}

class OpenmetadataLineageDispatcher(val config: OpenmetadataLineageDispatcherConfig) extends AbstractJsonLineageDispatcher with Logging {

  def this(configuration: Configuration) = this(new OpenmetadataLineageDispatcherConfig(configuration))

  private val SPARK_LINEAGE_SOURCE: String = "SparkLineage"
  private val TABLE_SEARCH_INDEX: String = "table_search_index"
  private val PIPELINE_SOURCE_TYPE: String = "Spark"

  override def name = "Openmetadata"


  override protected def send(data: String): Unit = {
    if (data.startsWith("ExecutionPlan")) {
      val jsonData = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
       createOrUpdatePipelineService()
       sendMetadataLineage(jsonData)

    }
  }
  private def getEntity(serviceName: String,databaseName: String,tableName: String): Map[String, ObjectNode] = {
    Try {
      val request = createGetTableRequest(Some(serviceName),databaseName,tableName)
      val response = sendSearchRequest(request)
      logWarning((s"Response keys: ${response.keys}"))
      val hitsResult = response("hits").asInstanceOf[Map[String, Any]]
      val totalHits = hitsResult("total").asInstanceOf[Map[String, Any]]("value").toString.toInt

      if (totalHits == 0) {
        logWarning(s"Failed to get id of table from OpenMetadata.")
        Map.empty[String, ObjectNode]
      } else {
        val tablesData = hitsResult("hits").asInstanceOf[List[Map[String, Any]]]
        logWarning(s"Found ${tablesData.length} tables")
        val resultMap = tablesData.map { tableHit =>
          val tableSource = tableHit("_source").asInstanceOf[Map[String, Any]]
          val tableName = tableSource.getOrElse("name", "unknown").toString
          val tableId = tableSource.getOrElse("id", "").toString
          val fullyQualifiedName = tableSource.getOrElse("fullyQualifiedName", "").toString
          val description = tableSource.getOrElse("description", "").toString
          val displayName = tableSource.getOrElse("displayName", tableName).toString
          val deleted = tableSource.getOrElse("deleted", false).asInstanceOf[Boolean]
          val objectMapper = new ObjectMapper()
          val entityJson = objectMapper.createObjectNode()
          entityJson.put("id", tableId)
          entityJson.put("name", tableName)
          entityJson.put("fullyQualifiedName", fullyQualifiedName)
          entityJson.put("deleted", deleted)
          entityJson.put("description", description)
          entityJson.put("displayName", displayName)
          entityJson.put("href", s"${config.hostPort}/api/v1/tables/$tableId")
          entityJson.put("inherited", true)
          entityJson.put("type", "table")
          tableName -> entityJson
        }.toMap
        resultMap
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        logInfo(s"Failed to get table entity from OpenMetadata: $exception")
        throw new Exception(exception)
    }
  }

  def sendMetadataLineage(jsonData: String): Unit = {
    try {
      val piplinen_id = createOrUpdatePipeline()
      logInfo(s"原始血缘数据: $jsonData")
      val objectMapper = new ObjectMapper()
      val operations = objectMapper.readTree(jsonData).get("operations")
      val write = operations.get("write")
      val targetType = getStringValue(write, "extra.destinationType")
      val targetDatabase = getStringValue(write, "params.table.identifier.database")
      val targetTableName = getStringValue(write, "params.table.identifier.table")
      val targetEntity: Map[String, ObjectNode] = getEntity(targetType, targetDatabase, targetTableName)

      val readsArray = operations.get("reads")

      for (i <- 0 until readsArray.size()) {
        val readObj = readsArray.get(i)
        val sourceType = getStringValue(readObj, "extra.sourceType")
        val sourceTable = getStringValue(readObj, "params.table.identifier.table")
        val sourceDatabase = getStringValue(readObj, "params.table.identifier.database")
        logInfo(s"源${i + 1}信息 - 类型: '$sourceType', 数据库: '$sourceDatabase', 表: '$sourceTable'")
        val sourceEntity: Map[String, ObjectNode] = getEntity(sourceType, sourceDatabase, sourceTable)

        if (sourceEntity.nonEmpty && targetEntity.nonEmpty) {
          val lineageRequest = createLineageRequest(piplinen_id, sourceEntity, targetEntity, jsonData, i)
          try {
            sendRequest(lineageRequest)
            logInfo(s"Successfully created lineage from $sourceTable to $targetTableName")
          } catch {
            case e: Exception =>
              logInfo(s"Failed to create lineage from $sourceTable to $targetTableName: ${e.getMessage}")
          }
        } else {
          logInfo(s"Skipping lineage creation: sourceEntity.isEmpty=${sourceEntity.isEmpty}, targetEntity.isEmpty=${targetEntity.isEmpty}")
        }
      }
    } catch {
      case e: Exception =>
        logInfo(s"解析血缘数据时发生异常: ${e.getMessage}")
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
      response.get("id") match {
        case Some(id) => id.toString
        case None => ""
      }
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
    val objectMapper = new ObjectMapper()
    objectMapper.writeValueAsString(obj)
  }

  def createPutRequest(path: String, jsonRequest: String): HttpRequest = {
    val fullUrl = s"${config.hostPort}$path"
    Http(fullUrl).put(jsonRequest).header("Content-Type", "application/json").header("Authorization", s"${config.jwtToken}")
  }

  private def sendRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.asString
      if (response.isSuccess) {
        if (response.body == null || response.body.trim.isEmpty) {
          logInfo(s"Warning: Empty response body received for request: ${request.url}")
          Map.empty[String, Any]
        } else {
          try {
            val objectMapper = new ObjectMapper()
            val jsonResponse = objectMapper.readTree(response.body)
            if (jsonResponse.has("id")) {
              Map("id" -> jsonResponse.get("id").asText())
            } else {
              logInfo(s"Warning: Response does not contain 'id' field: ${response.body}")
              Map.empty[String, Any]
            }
          } catch {
            case e: Exception =>
              logInfo(s"Failed to parse JSON response: ${response.body}, error: ${e.getMessage}")
              Map.empty[String, Any]
          }
        }
      } else {
        throw new RuntimeException(s"HTTP request failed with status: ${response.code}, body: ${response.body}")
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        logInfo(s"Failed to send HTTP request: ${exception.getMessage}")
        throw exception
    }
  }

  private def sendSearchRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.asString
      if (response.isSuccess) {
        val objectMapper = new ObjectMapper()
        val jsonResponse = objectMapper.readTree(response.body)
        convertJsonNodeToMap(jsonResponse)
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

  def createLineageRequest(pipenameId: String, fromEntity: Map[String, ObjectNode], toEntity: Map[String, ObjectNode], jsonData: String, sourceIndex: Int): HttpRequest = {
    val fromEntityJson = fromEntity.values.head
    val toEntityJson = toEntity.values.head
    val objectMapper = new ObjectMapper()

    val edgeJson = objectMapper.createObjectNode()
    edgeJson.set("toEntity", toEntityJson)
    edgeJson.set("fromEntity", fromEntityJson)
    val lineageDetailsJson = objectMapper.createObjectNode()
    lineageDetailsJson.set("pipeline", createPipelineEntityJson(pipenameId))
    lineageDetailsJson.put("source", SPARK_LINEAGE_SOURCE)

    val columnsLineageArray = objectMapper.createArrayNode()
    val columnLineageList = getColumnLevelLineage(jsonData, fromEntityJson.get("fullyQualifiedName").asText(), toEntityJson.get("fullyQualifiedName").asText(), sourceIndex)

    for (columnLineage <- columnLineageList) {
      val columnLineageJson = objectMapper.createObjectNode()
      val fromColumnsArray = objectMapper.createArrayNode()
      val fromColumns = columnLineage("fromColumns").asInstanceOf[List[String]]
      for (fromColumn <- fromColumns) {
        fromColumnsArray.add(fromColumn)
      }
      columnLineageJson.set("fromColumns", fromColumnsArray)
      columnLineageJson.put("toColumn", columnLineage("toColumn").asInstanceOf[String])
      columnsLineageArray.add(columnLineageJson)
    }

    lineageDetailsJson.set("columnsLineage", columnsLineageArray)
    edgeJson.set("lineageDetails", lineageDetailsJson)

    val requestJson = objectMapper.createObjectNode()
    requestJson.set("edge", edgeJson)

    val jsonRequest = toJsonString(requestJson)
    createPutRequest("/api/v1/lineage", jsonRequest)
  }

  private def createPipelineEntityJson(pipenameId: String): ObjectNode = {
    val objectMapper = new ObjectMapper()
    val pipelineJson = objectMapper.createObjectNode()
    pipelineJson.put("id", pipenameId)
    pipelineJson.put("type", "pipeline")
    pipelineJson
  }



  private def convertJsonNodeToMap(jsonNode: JsonNode): Map[String, Any] = {
    val result = scala.collection.mutable.Map[String, Any]()
    val fieldNames = jsonNode.fieldNames()
    import scala.collection.JavaConverters._
    for (fieldName <- fieldNames.asScala) {
      val value = jsonNode.get(fieldName)
      if (value == null || value.isNull) {
        result(fieldName) = null
      } else if (value.isValueNode) {
        if (value.isBoolean) {
          result(fieldName) = value.asBoolean()
        } else if (value.isNumber) {
          result(fieldName) = value.numberValue()
        } else {
          result(fieldName) = value.asText()
        }
      } else if (value.isObject) {
        // 递归处理嵌套的JSON对象
        result(fieldName) = convertJsonNodeToMap(value)
      } else if (value.isArray) {
        // 处理JSON数组
        val array = value.asInstanceOf[ArrayNode]
        val list = scala.collection.mutable.ListBuffer[Any]()
        for (i <- 0 until array.size()) {
          val element = array.get(i)
          if (element == null || element.isNull) {
            list += null
          } else if (element.isValueNode) {
            if (element.isBoolean) {
              list += element.asBoolean()
            } else if (element.isNumber) {
              list += element.numberValue()
            } else {
              list += element.asText()
            }
          } else if (element.isObject) {
            list += convertJsonNodeToMap(element)
          } else {
            list += element.toString
          }
        }
        result(fieldName) = list.toList
      } else {
        result(fieldName) = value.toString
      }
    }
    result.toMap
  }

  private def getColumnLevelLineage(jsonData: String, sourceTableFqn: String, targetTableFqn: String, sourceIndex: Int): List[Map[String, Any]] = {
    try {
      val objectMapper = new ObjectMapper()
      val json = objectMapper.readTree(jsonData).asInstanceOf[ObjectNode]

      val attributesMap = createAttributesMap(json)
      val op1OutputList = getOp1OutputListWithNames(json, attributesMap)
      val readsOutputInfo = getReadsOutputInfoWithNames(json, attributesMap)

      // 创建列转换映射
      val columnTransformationMap = createColumnTransformationMap(json, attributesMap)

      val columnLineageJson = generateColumnLineage(op1OutputList, readsOutputInfo, sourceTableFqn, targetTableFqn, columnTransformationMap)
      val lineageResults = mutable.ListBuffer[Map[String, Any]]()
      for (i <- 0 until columnLineageJson.size()) {
        val lineageObj = columnLineageJson.get(i).asInstanceOf[ObjectNode]
        val fromColumnsArray = lineageObj.get("fromColumns").asInstanceOf[ArrayNode]
        val toColumn = lineageObj.get("toColumn").asText()

        val fromColumns = (0 until fromColumnsArray.size()).map { j =>
          fromColumnsArray.get(j).asText()
        }.toList

        lineageResults += Map("fromColumns" -> fromColumns, "toColumn" -> toColumn)
      }


      lineageResults.toList
    } catch {
      case e: Exception =>

        List.empty[Map[String, Any]]
    }
  }

  private def createColumnTransformationMap(jsonObject: JsonNode, attributesMap: mutable.Map[String, String]): mutable.Map[String, String] = {
    val transformationMap = mutable.Map[String, String]()

    // 获取所有输出列
    val outputColumns = getOp1OutputListWithNames(jsonObject, attributesMap)

    // 获取所有输入列
    val inputColumns = mutable.Set[String]()
    if (jsonObject.has("operations")) {
      val operationsObj = jsonObject.get("operations")
      if (operationsObj.has("reads")) {
        val readsList = operationsObj.get("reads")
        for (i <- 0 until readsList.size()) {
          val readObj = readsList.get(i)
          if (readObj.has("output")) {
            val outputArray = readObj.get("output")
            for (j <- 0 until outputArray.size()) {
              inputColumns += outputArray.get(j).asText()
            }
          }
        }
      }
    }

    // 遍历所有属性，查找转换关系
    if (jsonObject.has("attributes")) {
      val attributesArray = jsonObject.get("attributes")
      for (i <- 0 until attributesArray.size()) {
        val attrObj = attributesArray.get(i)
        val attrId = attrObj.get("id").asText()
        val attrName = attrObj.get("name").asText()

        // 如果是输出列
        if (outputColumns.contains(attrName)) {
          // 查找转换关系
          val sourceAttrId = findSourceAttributeId(jsonObject, attrId)
          if (sourceAttrId.isDefined && inputColumns.contains(sourceAttrId.get)) {
            transformationMap(attrName) = attributesMap.getOrElse(sourceAttrId.get, sourceAttrId.get)
          } else if (inputColumns.contains(attrId)) {
            // 如果没有转换关系，但是本身就是输入列
            transformationMap(attrName) = attrName
          }
        }
      }
    }

    transformationMap
  }

  private def findSourceAttributeId(jsonObject: JsonNode, attrId: String): Option[String] = {
    // 使用递归和循环检测来查找源属性ID
    val visited = mutable.Set[String]()

    def findSource(attrId: String): Option[String] = {
      // 防止循环引用
      if (visited.contains(attrId)) {
        return None
      }
      visited.add(attrId)

      // 在attributes中查找
      if (jsonObject.has("attributes")) {
        val attributesArray = jsonObject.get("attributes")
        for (i <- 0 until attributesArray.size()) {
          val attrObj = attributesArray.get(i)
          if (attrId.equals(attrObj.get("id").asText())) {
            // 如果有childRefs，则继续查找
            if (attrObj.has("childRefs")) {
              val childRefsArray = attrObj.get("childRefs")
              for (j <- 0 until childRefsArray.size()) {
                val childRef = childRefsArray.get(j)
                if (childRef.has("__exprId")) {
                  val exprId = childRef.get("__exprId").asText()
                  // 在expressions中查找
                  if (jsonObject.has("expressions")) {
                    val expressionsObj = jsonObject.get("expressions")
                    if (expressionsObj.has("functions")) {
                      val functionsArray = expressionsObj.get("functions")
                      for (k <- 0 until functionsArray.size()) {
                        val funcObj = functionsArray.get(k)
                        if (exprId.equals(funcObj.get("id").asText())) {
                          // 查找函数的childRefs中的__attrId
                          if (funcObj.has("childRefs")) {
                            val funcChildRefsArray = funcObj.get("childRefs")
                            for (l <- 0 until funcChildRefsArray.size()) {
                              val funcChildRef = funcChildRefsArray.get(l)
                              if (funcChildRef.has("__attrId")) {
                                val sourceAttrId = funcChildRef.get("__attrId").asText()
                                // 递归查找
                                val result = findSource(sourceAttrId)
                                if (result.isDefined) {
                                  return result
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            // 如果没有childRefs，则返回当前属性ID
            return Some(attrId)
          }
        }
      }

      None
    }

    findSource(attrId)
  }

  private def getStringValue(json: JsonNode, path: String): String = {
    try {
      val keys = path.split("\\.")
      val lastIndex = keys.length - 1
      val parentObj = keys.dropRight(1).foldLeft(json) { (obj, key) =>
        if (obj != null && obj.has(key)) {
          obj.get(key)
        } else {
          null
        }
      }
      if (parentObj != null && parentObj.has(keys(lastIndex))) {
        val value = parentObj.get(keys(lastIndex)).asText()
        value
      } else {
        ""
      }
    } catch {
      case e: Exception =>
        ""
    }
  }



  private def createAttributesMap(jsonObject: JsonNode): mutable.Map[String, String] = {
    val attributesMap = mutable.Map[String, String]()

    if (jsonObject.has("attributes")) {
      val attributesArray = jsonObject.get("attributes")

      for (i <- 0 until attributesArray.size()) {
        val attrObj = attributesArray.get(i)
        attributesMap(attrObj.get("id").asText()) = attrObj.get("name").asText()
      }
    }

    attributesMap
  }


  private def getOp1OutputListWithNames(jsonObject: JsonNode, attributesMap: mutable.Map[String, String]): List[String] = {
    if (jsonObject.has("operations")) {
      val operationsObj = jsonObject.get("operations")

      if (operationsObj.has("other")) {
        val otherArray = operationsObj.get("other")

        for (i <- 0 until otherArray.size()) {
          val operationObj = otherArray.get(i)
          if ("op-1".equals(operationObj.get("id").asText()) && operationObj.has("output")) {
            val outputArray = operationObj.get("output")
            return (0 until outputArray.size()).map(j =>
              attributesMap.getOrElse(outputArray.get(j).asText(), outputArray.get(j).asText())
            ).toList
          }
        }
      }
    }

    List.empty[String]
  }

  private def getReadsOutputInfoWithNames(jsonObject: JsonNode, attributesMap: mutable.Map[String, String]): ArrayNode = {
    val objectMapper = new ObjectMapper()
    val readsArray = objectMapper.createArrayNode()
    val uniqueEntries = mutable.Set[String]()

    if (jsonObject.has("operations")) {
      val operationsObj = jsonObject.get("operations")

      if (operationsObj.has("reads")) {
        val readsList = operationsObj.get("reads")

        for (i <- 0 until readsList.size()) {
          val readObj = readsList.get(i)
          val readInfo = objectMapper.createObjectNode()

          // 提取数据库名和表名
          if (readObj.has("params") && readObj.get("params").has("table")) {
            val tableObj = readObj.get("params").get("table")
            if (tableObj.has("identifier")) {
              val identifierObj = tableObj.get("identifier")
              val database = identifierObj.get("database").asText()
              val table = identifierObj.get("table").asText()
              readInfo.put("database", database)
              readInfo.put("table", table)

              // 提取output列表并将列ID转换为列名
              if (readObj.has("output")) {
                val outputArray = readObj.get("output")
                val outputNamesList = (0 until outputArray.size()).map(j =>
                  attributesMap.getOrElse(outputArray.get(j).asText(), outputArray.get(j).asText())
                ).toList.sorted

                val uniqueKey = database + "|" + table + "|" + outputNamesList.mkString(",")

                if (!uniqueEntries.contains(uniqueKey)) {
                  uniqueEntries.add(uniqueKey)

                  val outputNamesArray = objectMapper.createArrayNode()
                  outputNamesList.foreach(outputNamesArray.add)
                  readInfo.set("output", outputNamesArray)

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

  private def getFilteredFunctionsWithNames(jsonObject: JsonNode, attributesMap: mutable.Map[String, String]): ArrayNode = {
    val objectMapper = new ObjectMapper()
    val filteredArray = objectMapper.createArrayNode()
    val uniqueEntries = mutable.Set[String]()

    val expressionsNode = Option(jsonObject.get("expressions")).getOrElse(objectMapper.createObjectNode())
    val functionsArray = Option(expressionsNode.get("functions")).getOrElse(objectMapper.createArrayNode())

    for (i <- 0 until functionsArray.size()) {
      val func = functionsArray.get(i)

      val childRefsNode = Option(func.get("childRefs")).getOrElse(objectMapper.createArrayNode())
      var hasAttrIdWithAttr = false

      for (j <- 0 until childRefsNode.size()) {
        val elem = childRefsNode.get(j)
        if (elem.has("__attrId") && elem.get("__attrId").asText().contains("attr")) {
          hasAttrIdWithAttr = true
        }
      }

      val extraNode = Option(func.get("extra")).getOrElse(objectMapper.createObjectNode())
      val hasAliasTypeHint = "expr.Alias".equals(extraNode.get("_typeHint").asText())

      if (hasAttrIdWithAttr && hasAliasTypeHint) {
        val paramsNode = Option(func.get("params")).getOrElse(objectMapper.createObjectNode())
        val paramsName = if (paramsNode.has("name")) paramsNode.get("name").asText() else ""

        val columnNames = mutable.ListBuffer[String]()
        for (j <- 0 until childRefsNode.size()) {
          val elem = childRefsNode.get(j)
          if (elem.has("__attrId")) {
            val attrId = elem.get("__attrId").asText()
            if (attrId.contains("attr")) {
              columnNames += attributesMap.getOrElse(attrId, attrId)
            }
          }
        }
        val sortedColumnNames = columnNames.toList.sorted

        val uniqueKey = paramsName + "|" + sortedColumnNames.mkString(",")

        if (!uniqueEntries.contains(uniqueKey)) {
          uniqueEntries.add(uniqueKey)

          val resultObj = objectMapper.createObjectNode()
          resultObj.put("params_name", paramsName)

          val columnNamesArray = objectMapper.createArrayNode()
          sortedColumnNames.foreach(columnNamesArray.add)
          resultObj.set("column_names", columnNamesArray)

          filteredArray.add(resultObj)
        }
      }
    }

    filteredArray
  }

  private def generateColumnLineage(writeColumns: List[String],
    readTablesInfo: ArrayNode,
    sourceTableFqn: String,
    targetTableFqn: String,
    columnTransformationMap: mutable.Map[String, String]): ArrayNode = {

    val objectMapper = new ObjectMapper()

    // 使用传入的targetTableFqn作为目标表
    if (targetTableFqn.isEmpty) return objectMapper.createArrayNode()

    // 使用传入的sourceTableFqn作为源表
    if (sourceTableFqn.isEmpty) return objectMapper.createArrayNode()

    val readTableColumns = (0 until readTablesInfo.size()).map { i =>
      val obj = readTablesInfo.get(i)
      val database = obj.get("database").asText()
      val table = obj.get("table").asText()
      val outputArray = obj.get("output")
      val cols = (0 until outputArray.size()).map(j => outputArray.get(j).asText()).toList
      (s"$database.$table", cols)
    }.toMap


    val resultArray = objectMapper.createArrayNode()
    val processedPairs = mutable.Set[String]() // 用于记录已处理的列对，避免重复

    writeColumns.foreach { writeCol =>
      // 使用传入的targetTableFqn构建toColumn
      val target = s"$targetTableFqn.$writeCol"

      // 使用列转换映射来找到源列
      val sourceCol = columnTransformationMap.getOrElse(writeCol, writeCol)

      readTableColumns.foreach {
        case (table, cols) =>
          if(sourceTableFqn.endsWith(table)){
            if (cols.contains(sourceCol) ) {
              // 检查是否已经处理过这个列对
              val pairKey = s"$sourceCol-$writeCol"
              if (!processedPairs.contains(pairKey)) {
                processedPairs.add(pairKey)

                val lineageObj = objectMapper.createObjectNode()
                val fromColumnsArray = objectMapper.createArrayNode()
                // 使用传入的sourceTableFqn构建fromColumns
                fromColumnsArray.add(s"$sourceTableFqn.$sourceCol")

                lineageObj.set("fromColumns", fromColumnsArray)
                lineageObj.put("toColumn", target)
                resultArray.add(lineageObj)
              }
            }
          }
      }
    }

    resultArray
  }



}

