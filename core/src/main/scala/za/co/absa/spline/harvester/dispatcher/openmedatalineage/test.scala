package za.co.absa.spline.harvester.dispatcher.openmedatalineage

import com.alibaba.fastjson2.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang.StringUtils
import scalaj.http.{Http, HttpRequest}

import java.net.URI
import java.util
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
import scala.collection.mutable
object test {
  private val SPARK_LINEAGE_SOURCE: String = "SparkLineage"
  private val TABLE_SEARCH_INDEX: String = "table_search_index"
  private val PIPELINE_SOURCE_TYPE: String = "Spark"

  def main(args: Array[String]): Unit = {



//    if (data.startsWith("ExecutionPlan")) {
//      val jsonData = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
//      //创建pipeline Service  注意这里的id之后放置列级别的血缘会用到
//      val pipserviceId = createOrUpdatePipelineService()
//      //从这个jsondata中解析出sourceentity,targetentity,sourcetable,targettable 构建血缘
//      sendMetadataLineage(jsonData,pipserviceId)
//
//    }
  }

  private def getEntity(serviceName: String,databaseName: String,tableName: String): Map[String, JSONObject] = {
    Try {
      val request = createGetTableRequest(Some(serviceName),databaseName,tableName)
      val response = sendSearchRequest(request)
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
      case Some(service) => s"$service.*.$databaseName.*$tableName"
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
    request = request.header("Authorization",  "eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTM2OTUwNTYsImV4cCI6bnVsbH0.N4hARFRrfI06NVZGJIYg_bEg2WY-Z4AoITdVScstxG0NEcU_17zsP1yyO05OqH867QeEqKczu1pZ4XUU1DmR7INxhd3gyF5peO94K8tRjpWcOIxQQdyTGPRy_SfBapRiNhND5OHEAk2aq_z4mBKmnP83Kwq0jwdKhE9xz7_PFtRhGN1vdEzPuOL-6A-WKjh7Y3ixyHqOdbyPfm-XDth2yPShqJ_gNArvWhBkOZxvbpylE6eOFDj__woChwB6dtpYAVwXHP7MGxTLUzBiyc8YQojfmO1hSRIu6hSXwntBqQlzyiyohwjmR3O6tgJrEkYou59tkfW_BuYo6jo_QJtDqQ")
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
        throw new OpenLineageClientException(e)
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
      response.get("id").toString
    } catch {
      case e: Exception =>
        println(s"Failed to create/update service pipeline ${"pipeline_service"} in OpenMetadata: ", e)
        throw new OpenLineageClientException(e)
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

  def toJsonString(obj: AnyRef): String = JSON.toJSONString(obj)

  def createPutRequest(path: String, jsonRequest: String): HttpRequest = {
    val fullUrl = s"http://172.16.0.179:8585$path"
    Http(fullUrl).put(jsonRequest).header("Content-Type", "application/json")
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

  private def sendSearchRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.header("Authorization", s"Bearer eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTM2OTUwNTYsImV4cCI6bnVsbH0.N4hARFRrfI06NVZGJIYg_bEg2WY-Z4AoITdVScstxG0NEcU_17zsP1yyO05OqH867QeEqKczu1pZ4XUU1DmR7INxhd3gyF5peO94K8tRjpWcOIxQQdyTGPRy_SfBapRiNhND5OHEAk2aq_z4mBKmnP83Kwq0jwdKhE9xz7_PFtRhGN1vdEzPuOL-6A-WKjh7Y3ixyHqOdbyPfm-XDth2yPShqJ_gNArvWhBkOZxvbpylE6eOFDj__woChwB6dtpYAVwXHP7MGxTLUzBiyc8YQojfmO1hSRIu6hSXwntBqQlzyiyohwjmR3O6tgJrEkYou59tkfW_BuYo6jo_QJtDqQ").asString
      if (response.isSuccess) {
        val jsonResponse = JSON.parseObject(response.body)
        // 将 JSONObject 转换为 Map
        jsonResponse.asScala.toMap
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

        if (sourceEntity.nonEmpty && targetEntity.nonEmpty) {
          val lineageRequest = createLineageRequest(pipserviceId, sourceEntity, targetEntity, jsonData, i)

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

    val edgeMap = Map(
      "toEntity" -> convertJSONObjectToMap(toEntityJson),
      "fromEntity" -> convertJSONObjectToMap(fromEntityJson),
      "lineageDetails" -> Map(
        "pipeline" -> createPipelineEntityMap(pipserviceId),
        "source" -> SPARK_LINEAGE_SOURCE,
        "columnsLineage" -> getColumnLevelLineage(jsonData, fromEntityJson.getString("fullyQualifiedName"), toEntityJson.getString("fullyQualifiedName"), sourceIndex)
      )
    )

    val requestMap = Map("edge" -> edgeMap)
    val jsonRequest = toJsonString(requestMap)
    createPutRequest("/api/v1/lineage", jsonRequest)
  }

  private def createPipelineEntityMap(pipserviceId: String): Map[String, Any] = {
    Map(
      "id" -> pipserviceId,
      "type" -> "pipelineService",
      "name" -> "pipeline_service",
      "fullyQualifiedName" -> "pipeline_service",
      "href" -> s"http://172.16.0.179:8585/api/v1/services/pipelineServices/${pipserviceId}",
      "deleted" -> false,
      "inherited" -> true
    )
  }

  private def convertJSONObjectToMap(jsonObject: JSONObject): Map[String, Any] = {
    Map(
      "id" -> jsonObject.getString("id"),
      "name" -> jsonObject.getString("name"),
      "fullyQualifiedName" -> jsonObject.getString("fullyQualifiedName"),
      "deleted" -> jsonObject.getBoolean("deleted"),
      "description" -> jsonObject.getString("description"),
      "displayName" -> jsonObject.getString("displayName"),
      "href" -> jsonObject.getString("href"),
      "inherited" -> jsonObject.getBoolean("inherited"),
      "type" -> jsonObject.getString("type")
    )
  }

  private def getColumnLevelLineage(jsonData: String, sourceTableFqn: String, targetTableFqn: String, sourceIndex: Int): List[Map[String, Any]] = {
    try {
      val json = JSON.parseObject(jsonData)
      val operations = json.getJSONObject("operations")
      val reads = operations.getJSONArray("reads")
      val write = operations.getJSONObject("write")
      val attributeMap = buildAttributeMap(json.getJSONArray("attributes"))
      val otherOps = operations.getJSONArray("other")

      val lineageResults = mutable.ListBuffer[Map[String, Any]]()

      if (reads != null && reads.size() > sourceIndex) {
        // 获取指定索引的源表的输出列
        val currentRead = reads.getJSONObject(sourceIndex)
        val sourceOutputAttrs = Option(currentRead.getJSONArray("output")).map(_.asScala.toList.map(_.toString)).getOrElse(List.empty)

        // 获取最终写入操作的输入列（这些是实际写入目标表的列）
        val writeInputAttrs = getWriteInputAttributes(write, otherOps)

        // 构建列级血缘关系
        val columnMappings = traceColumnLineage(sourceOutputAttrs, writeInputAttrs, otherOps, attributeMap)

        columnMappings.foreach { case (sourceAttrs, targetAttr) =>
          val sourceColumns = sourceAttrs.map(attrId => {
            val columnName = attributeMap.getOrElse(attrId, attrId)
            s"$sourceTableFqn.$columnName"
          })
          val targetColumnName = attributeMap.getOrElse(targetAttr, targetAttr)

          lineageResults += Map(
            "fromColumns" -> sourceColumns,
            "toColumn" -> s"$targetTableFqn.$targetColumnName"
          )
        }
      }

      println(s"Generated ${lineageResults.size} column lineage entries for source table index $sourceIndex")
      lineageResults.toList
    } catch {
      case e: Exception =>
        println(s"Failed to parse column level lineage: ${e.getMessage}")
        fallbackToSimpleMapping(jsonData, sourceTableFqn, targetTableFqn, sourceIndex)
    }
  }

  private def getWriteInputAttributes(write: JSONObject, otherOps: com.alibaba.fastjson2.JSONArray): List[String] = {
    // 从写操作的子操作中获取输入属性
    val childIds = Option(write.getJSONArray("childIds"))
      .map(_.asScala.toList.map(_.toString))
      .getOrElse(List.empty)

    if (childIds.nonEmpty && otherOps != null) {
      // 找到写操作的直接子操作
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

    // 简化版本：假设列的顺序对应关系
    // 在实际实现中，需要分析 Project 操作的 projectList 来建立精确映射
    val mappings = mutable.ListBuffer[(List[String], String)]()

    val minSize = Math.min(sourceAttrs.size, targetAttrs.size)
    for (i <- 0 until minSize) {
      mappings += ((List(sourceAttrs(i)), targetAttrs(i)))
    }

    mappings.toList
  }

  private def fallbackToSimpleMapping(jsonData: String, sourceTableFqn: String, targetTableFqn: String, sourceIndex: Int): List[Map[String, Any]] = {
    try {
      val json = JSON.parseObject(jsonData)
      val operations = json.getJSONObject("operations")
      val reads = operations.getJSONArray("reads")
      val attributeMap = buildAttributeMap(json.getJSONArray("attributes"))

      val lineageResults = mutable.ListBuffer[Map[String, Any]]()

      if (reads != null && reads.size() > sourceIndex) {
        val currentRead = reads.getJSONObject(sourceIndex)
        val readOutputAttrs = Option(currentRead.getJSONArray("output"))
          .map(_.asScala.toList.map(_.toString))
          .getOrElse(List.empty)

        readOutputAttrs.foreach { attrId =>
          val columnName = attributeMap.getOrElse(attrId, attrId)

          lineageResults += Map(
            "fromColumns" -> List(s"$sourceTableFqn.$columnName"),
            "toColumn" -> s"$targetTableFqn.$columnName"
          )
        }
      }

      println(s"使用简单列映射作为降级方案 (源表索引: $sourceIndex)")
      lineageResults.toList
    } catch {
      case e: Exception =>
        println(s"降级方案也失败了: ${e.getMessage}")
        List.empty[Map[String, Any]]
    }
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
