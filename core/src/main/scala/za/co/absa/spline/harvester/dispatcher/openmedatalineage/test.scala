package za.co.absa.spline.harvester.dispatcher.openmedatalineage

import com.alibaba.fastjson2.{JSON, JSONObject}
import org.apache.commons.lang.StringUtils
import scalaj.http.{Http, HttpRequest}
import java.net.URI
import java.util
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer



object test {
  private val SPARK_LINEAGE_SOURCE: String = "SparkLineage"
  private val TABLE_SEARCH_INDEX: String = "table_search_index"
  private val PIPELINE_SOURCE_TYPE: String = "Spark"

  def main(args: Array[String]): Unit = {
    // 读取 xueyuan.txt 文件中的 JSON 数据
    val source = scala.io.Source.fromFile("/Users/zhouzhou/IdeaProjects/gouzheng/spline-spark-agent/core/src/main/scala/za/co/absa/spline/harvester/dispatcher/openmedatalineage/xueyuan.txt")
    val data = try source.mkString finally source.close()
    
    val jsonData = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
      //创建pipeline Service  注意这里的id之后放置列级别的血缘会用到
      val pipserviceId = createOrUpdatePipelineService()
      //从这个jsondata中解析出sourceentity,targetentity,sourcetable,targettable 构建血缘
      sendMetadataLineage(jsonData,pipserviceId)

    
  }

  private def getEntity(serviceName: String,databaseName: String,tableName: String): Map[String, JSONObject] = {
    Try {
      val request = createGetTableRequest(Some(serviceName),databaseName,tableName)
      val response = sendSearchRequest(request)
      println(s"Response: $response")
      val hitsResult = response.getJSONObject("hits")
      val totalHits = hitsResult.getJSONObject("total").getIntValue("value")

      if (totalHits == 0) {
        println(s"Failed to get id of table from OpenMetadata.")
        Map.empty[String, JSONObject]
      } else {
        val tablesData = hitsResult.getJSONArray("hits")
        println(s"Found ${tablesData.size()} tables")
        val resultMap = (0 until tablesData.size()).map { i =>
          val tableHit = tablesData.getJSONObject(i)
          val tableSource = tableHit.getJSONObject("_source")
          val tableName = tableSource.getString("name")
          val tableId = tableSource.getString("id")
          val fullyQualifiedName = tableSource.getString("fullyQualifiedName")
          val description = Option(tableSource.getString("description")).getOrElse("")
          val displayName = Option(tableSource.getString("displayName")).getOrElse(tableName)
          val deleted = tableSource.getBooleanValue("deleted")
          val entityJson = new JSONObject()
          entityJson.put("id", tableId: Object)
          entityJson.put("name", tableName: Object)
          entityJson.put("fullyQualifiedName", fullyQualifiedName: Object)
          entityJson.put("deleted", java.lang.Boolean.valueOf(deleted): Object)
          entityJson.put("description", description: Object)
          entityJson.put("displayName", displayName: Object)
          entityJson.put("href", s"http://172.16.0.179:8585/api/v1/tables/$tableId": Object)
          entityJson.put("inherited", java.lang.Boolean.valueOf(true): Object)
          entityJson.put("type", "table": Object)
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
    val baseUri = new URI("http://172.16.0.179:8585")
    val fullUrl = s"${baseUri.getScheme}://${baseUri.getHost}:${baseUri.getPort}/$path"
    var request = Http(fullUrl).params(queryParams).header("Accept", "application/json").header("Content-Type", "application/json")
    request
  }

  private def createOrUpdatePipeline(): String = {
    try {
      val request = createPipelineRequest()
      val response = sendRequest(request)
      response.get("id").toString
    } catch {
      case e: Exception =>
        println(s"Failed to create/update pipeline pipeline_name in OpenMetadata: ", e)
        ""
    }
  }


  def createPipelineRequest(): HttpRequest = {
    val requestMap = scala.collection.mutable.Map[String, Object]()
    val pipelineDescription = "pipeline_description"
    requestMap.put("name", "pipeline_name")
    requestMap.put("sourceUrl", "http://172.168.0.179:8585/pipeline_service")
    if (pipelineDescription!= null &&pipelineDescription.nonEmpty) {
      requestMap.put("description", pipelineDescription)
    }

    requestMap.put("service", "pipeline_service")
    val jsonRequest = toJsonString(requestMap)
    createPutRequest("/api/v1/pipelines", jsonRequest)
  }



  private def createOrUpdatePipelineService(): String = {
    try {
      val request = createPipelineServiceRequest()
      val response = sendRequest(request)
      val serviceId = response("id").toString
      println(s"Successfully created/updated pipeline service with ID: $serviceId")
      serviceId
    } catch {
      case e: Exception =>
        println(s"Failed to create/update service pipeline ${"pipeline_service"} in OpenMetadata: ", e)
        ""
    }
  }


  def createPipelineServiceRequest(): HttpRequest = {
    val requestMap = new util.HashMap[String, AnyRef]
    requestMap.put("name", "pipeline_service")
    requestMap.put("serviceType", PIPELINE_SOURCE_TYPE)
    val connectionConfig = new util.HashMap[String, AnyRef]
    val connectionType = new util.HashMap[String, AnyRef]
    connectionType.put("type", PIPELINE_SOURCE_TYPE)
    connectionConfig.put("config", connectionType)
    requestMap.put("connection", connectionConfig)
    val jsonRequest = toJsonString(requestMap)
    createPutRequest("/api/v1/services/pipelineServices", jsonRequest)
  }



  def createPutRequest(path: String, jsonRequest: String): HttpRequest = {
    val fullUrl = s"http://172.16.0.179:8585$path"
    Http(fullUrl).put(jsonRequest).header("Content-Type", "application/json")
  }

  private def sendSearchRequest(request: HttpRequest): JSONObject = {
    Try {
      val response = request.header("Authorization", s"Bearer eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTM2OTUwNTYsImV4cCI6bnVsbH0.N4hARFRrfI06NVZGJIYg_bEg2WY-Z4AoITdVScstxG0NEcU_17zsP1yyO05OqH867QeEqKczu1pZ4XUU1DmR7INxhd3gyF5peO94K8tRjpWcOIxQQdyTGPRy_SfBapRiNhND5OHEAk2aq_z4mBKmnP83Kwq0jwdKhE9xz7_PFtRhGN1vdEzPuOL-6A-WKjh7Y3ixyHqOdbyPfm-XDth2yPShqJ_gNArvWhBkOZxvbpylE6eOFDj__woChwB6dtpYAVwXHP7MGxTLUzBiyc8YQojfmO1hSRIu6hSXwntBqQlzyiyohwjmR3O6tgJrEkYou59tkfW_BuYo6jo_QJtDqQ").asString
      if (response.isSuccess) {
        JSON.parseObject(response.body)
      } else {
        throw new RuntimeException(s"HTTP search request failed with status: ${response.code}")
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"Failed to send search HTTP request: ${exception.getMessage}")
        throw exception
    }
  }

  private def sendRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.header("Authorization", s"Bearer eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTM2OTUwNTYsImV4cCI6bnVsbH0.N4hARFRrfI06NVZGJIYg_bEg2WY-Z4AoITdVScstxG0NEcU_17zsP1yyO05OqH867QeEqKczu1pZ4XUU1DmR7INxhd3gyF5peO94K8tRjpWcOIxQQdyTGPRy_SfBapRiNhND5OHEAk2aq_z4mBKmnP83Kwq0jwdKhE9xz7_PFtRhGN1vdEzPuOL-6A-WKjh7Y3ixyHqOdbyPfm-XDth2yPShqJ_gNArvWhBkOZxvbpylE6eOFDj__woChwB6dtpYAVwXHP7MGxTLUzBiyc8YQojfmO1hSRIu6hSXwntBqQlzyiyohwjmR3O6tgJrEkYou59tkfW_BuYo6jo_QJtDqQ").asString
      if (response.isSuccess) {
        val jsonResponse = JSON.parseObject(response.body)
        Map("id" -> jsonResponse.getString("id"))
      } else {
        throw new RuntimeException(s"HTTP request failed with status: ${response.code}")
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        println(s"Failed to send HTTP request: ${exception.getMessage}")
        throw exception
    }
  }


  def sendMetadataLineage(jsonData: String,pipserviceId: String): Unit = {
    try {
      createOrUpdatePipeline()
      println(s"原始血缘数据: $jsonData")
      val operations = JSON.parseObject(jsonData).getJSONObject("operations")
      val write = operations.getJSONObject("write")
      val targetType = getStringValue(write, "extra.destinationType")
      val targetDatabase = getStringValue(write, "params.table.identifier.database")
      val targetTableName = getStringValue(write, "params.table.identifier.table")
      val targetEntity: Map[String, JSONObject] = getEntity(targetType, targetDatabase, targetTableName)

      val readsArray = operations.getJSONArray("reads")

      for (i <- 0 until readsArray.size()) {
        val readObj = readsArray.getJSONObject(i)
        val sourceType = getStringValue(readObj, "extra.sourceType")
        val sourceTable = getStringValue(readObj, "params.table.identifier.table")
        val sourceDatabase = getStringValue(readObj, "params.table.identifier.database")
        println(s"源${i + 1}信息 - 类型: '$sourceType', 数据库: '$sourceDatabase', 表: '$sourceTable'")
        val sourceEntity: Map[String, JSONObject] = getEntity(sourceType, sourceDatabase, sourceTable)
        var lineageRequest :HttpRequest = null
        if (sourceEntity.nonEmpty && targetEntity.nonEmpty) {
           lineageRequest = createLineageRequest(pipserviceId, sourceEntity, targetEntity, jsonData, i)
          try {
            val response = sendRequest(lineageRequest)
            println(s"Successfully created lineage from $sourceTable to $targetTableName")
          } catch {
            case e: Exception =>
              println(s"Failed to create lineage from $sourceTable to $targetTableName: ${e.getMessage}")
          }
        } else {
          println(s"Skipping lineage creation: sourceEntity.isEmpty=${sourceEntity.isEmpty}, targetEntity.isEmpty=${targetEntity.isEmpty}")
        }
      }
    } catch {
      case e: Exception =>
        println(s"解析血缘数据时发生异常: ${e.getMessage}")
    }
  }


  def createLineageRequest(pipserviceId: String, fromEntity: Map[String, JSONObject], toEntity: Map[String, JSONObject], jsonData: String, sourceIndex: Int): HttpRequest = {
    val fromEntityJson = fromEntity.values.head
    val toEntityJson = toEntity.values.head

    // 使用 Java HashMap 确保正确的 JSON 序列化
    val lineageDetailsMap = new java.util.HashMap[String, Any]()
    lineageDetailsMap.put("pipeline", createPipelineEntityMap(pipserviceId))
    lineageDetailsMap.put("source", SPARK_LINEAGE_SOURCE)
    lineageDetailsMap.put("columnsLineage", getColumnLevelLineage(jsonData, fromEntityJson.getString("fullyQualifiedName"), toEntityJson.getString("fullyQualifiedName"), sourceIndex))
    val edgeMap = new java.util.HashMap[String, Any]()
    edgeMap.put("toEntity", convertJSONObjectToMap(toEntityJson))
    edgeMap.put("fromEntity", convertJSONObjectToMap(fromEntityJson))
    edgeMap.put("lineageDetails", lineageDetailsMap)

    val requestMap = new java.util.HashMap[String, Any]()
    requestMap.put("edge", edgeMap)
    val jsonRequest = toJsonString(requestMap)

    createPutRequest("/api/v1/lineage", jsonRequest)
  }

  private def createPipelineEntityMap(pipserviceId: String): java.util.HashMap[String, Any] = {
    val map = new java.util.HashMap[String, Any]()
    map.put("id", pipserviceId)
    map.put("type", "pipelineService")
    map.put("name", "pipeline_service")
    map.put("fullyQualifiedName", "pipeline_service")
    map.put("href", s"http://172.16.0.179:8585/api/v1/services/pipelineServices/${pipserviceId}")
    map.put("deleted", java.lang.Boolean.valueOf(false))
    map.put("inherited", java.lang.Boolean.valueOf(true))
    map
  }

  private def convertJSONObjectToMap(jsonObject: JSONObject): java.util.HashMap[String, Any] = {
    val map = new java.util.HashMap[String, Any]()
    map.put("id", jsonObject.getString("id"))
    map.put("name", jsonObject.getString("name"))
    map.put("fullyQualifiedName", jsonObject.getString("fullyQualifiedName"))
    map.put("deleted", jsonObject.getBoolean("deleted"))
    map.put("description", jsonObject.getString("description"))
    map.put("displayName", jsonObject.getString("displayName"))
    map.put("href", jsonObject.getString("href"))
    map.put("inherited", jsonObject.getBoolean("inherited"))
    map.put("type", jsonObject.getString("type"))
    map
  }

  private def getColumnLevelLineage(jsonData: String, sourceTableFqn: String, targetTableFqn: String, sourceIndex: Int): List[java.util.HashMap[String, Any]] = {
    try {
      val json = JSON.parseObject(jsonData)
      val operations = json.getJSONObject("operations")
      val reads = operations.getJSONArray("reads")
      val write = operations.getJSONObject("write")
      val attributeMap = buildAttributeMap(json.getJSONArray("attributes"))
      val otherOps = operations.getJSONArray("other")
      
      val lineResults = new ListBuffer[java.util.HashMap[String, Any]]

      if (reads != null && reads.size() > sourceIndex) {
        val currentRead = reads.getJSONObject(sourceIndex)
        val sourceOutputAttrs = if (currentRead.getJSONArray("output") != null) {
          currentRead.getJSONArray("output").asScala.toList.map(_.toString)
        } else {
          List.empty[String]
        }
        val writeInputAttrs = getWriteInputAttributes(write, otherOps)
        val columnMappings = traceColumnLineage(sourceOutputAttrs, writeInputAttrs, otherOps, attributeMap)
        
        val mappingIterator = columnMappings.iterator
        while (mappingIterator.hasNext) {
          val mapping = mappingIterator.next()
          val sourceAttrs = mapping._1
          val targetAttr = mapping._2
          
          val sourceColumns = sourceAttrs.map(attrId => {
            val columnName = attributeMap.getOrElse(attrId, attrId)
            s"$sourceTableFqn.$columnName"
          })
          
          val targetColumnName = attributeMap.getOrElse(targetAttr, targetAttr)
          val lineageMap = new java.util.HashMap[String, Any]()
          val fromColumnsArray = new java.util.ArrayList[String]()
          
          val columnsIterator = sourceColumns.iterator
          while (columnsIterator.hasNext) {
            fromColumnsArray.add(columnsIterator.next())
          }
          
          lineageMap.put("fromColumns", fromColumnsArray)
          lineageMap.put("toColumn", s"$targetTableFqn.$targetColumnName")
          lineResults.append(lineageMap)
        }
      }
      
      val resultList = lineResults.toList
      println(s"Generated ${resultList.size} column lineage entries for source table index $sourceIndex")
      resultList
    } catch {
      case e: Exception =>
        println(s"Failed to parse column level lineage: ${e.getMessage}")
        List.empty[java.util.HashMap[String, Any]]
    }
  }

  private def getWriteInputAttributes(write: JSONObject, otherOps: com.alibaba.fastjson2.JSONArray): List[String] = {
    val childIds = Option(write.getJSONArray("childIds"))
      .map(_.asScala.toList.map(_.toString))
      .getOrElse(List.empty)

    if (childIds.nonEmpty && otherOps != null) {
      val directChild = otherOps.asScala.find { op =>
        val opObj = op.asInstanceOf[JSONObject]
        childIds.contains(opObj.getString("id"))
      }
      directChild match {
        case Some(childOp) =>
          val childOpObj = childOp.asInstanceOf[JSONObject]
          Option(childOpObj.getJSONArray("output"))
            .map(_.asScala.toList.map(_.toString))
            .getOrElse(List.empty)
        case None => List.empty
      }
    } else {
      List.empty
    }
  }

  private def traceColumnLineage(
    sourceAttrs: List[String],
    targetAttrs: List[String],
    otherOps: com.alibaba.fastjson2.JSONArray,
    attributeMap: Map[String, String]
  ): List[(List[String], String)] = {
    
    val mappings = new ListBuffer[(List[String], String)]
    val minSize = Math.min(sourceAttrs.size, targetAttrs.size)
    
    var i = 0
    while (i < minSize) {
      val sourceAttr = sourceAttrs(i)
      val targetAttr = targetAttrs(i)
      mappings.append((List(sourceAttr), targetAttr))
      i += 1
    }
    
    mappings.toList
  }

  def toJsonString(obj: AnyRef): String = {
    import com.alibaba.fastjson2.JSONWriter
    // 使用 WriteMapNullValue 确保正确序列化
    JSON.toJSONString(obj, JSONWriter.Feature.WriteMapNullValue)
  }

  private def buildAttributeMap(attributesJson: com.alibaba.fastjson2.JSONArray): Map[String, String] = {
    if (attributesJson == null) {
      return Map.empty[String, String]
    }

    attributesJson.asScala.collect {
      case attr: JSONObject =>
        val id = attr.getString("id")
        val name = attr.getString("name")
        if (id != null && name != null) {
          id -> name
        } else {
          null
        }
    }.filter(_ != null).toMap
  }

  private def getStringValue(json: JSONObject, path: String): String = {
    try {
      val keys = path.split("\\.")
      val lastIndex = keys.length - 1
      val parentObj = keys.dropRight(1).foldLeft(json) { (obj, key) =>
        if (obj != null && obj.containsKey(key)) {
          obj.getJSONObject(key)
        } else {
          null
        }
      }
      if (parentObj != null && parentObj.containsKey(keys(lastIndex))) {
        val value = parentObj.getString(keys(lastIndex))
        value
      } else {
        ""
      }
    } catch {
      case e: Exception =>
        ""
    }
  }
  

}
