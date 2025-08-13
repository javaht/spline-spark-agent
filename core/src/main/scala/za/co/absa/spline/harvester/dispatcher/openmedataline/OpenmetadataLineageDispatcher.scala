
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
package za.co.absa.spline.harvester.dispatcher.openmedataline
import org.apache.commons.configuration.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import okhttp3._
import scala.util.{Try, Success, Failure}
import scala.collection.mutable
import com.alibaba.fastjson2.{JSON, JSONObject}
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import java.util

class OpenmetadataLineageDispatcher(

  val config: OpenmetadataLineageDispatcherConfig) extends AbstractJsonLineageDispatcher with Logging {

  def this(configuration: Configuration) = this(
    new OpenmetadataLineageDispatcherConfig(configuration)
  )

  // 常量定义
  private val SPARK_LINEAGE_SOURCE: String = "SparkLineage"
  private val TABLE_SEARCH_INDEX: String = "table_search_index"
  private val CONTAINER_SEARCH_INDEX: String = "container_search_index"
  private val PIPELINE_SOURCE_TYPE: String = "Spark"

  private val HTTP_TIMEOUT_SECONDS = 30
  private val CONTENT_TYPE_JSON = "application/json"
  private val tableCache = mutable.Map[String, Map[String, Map[String, String]]]()
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
  createPipelineServiceRequest()








  def createPipelineServiceRequest(): Option[Request] = {
    Try {
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

    } match {
      case Success(Some(request)) => Some(request)
      case Success(None) => None
      case Failure(exception) =>
        log.error(s"Failed to create pipeline service request: ${exception.getMessage}")
        exception.printStackTrace()
        None
    }
  }

  def toJsonString(obj: AnyRef): String = JSON.toJSONString(obj)

  def createPutRequest(path: String, jsonRequest: String): Option[Request] = {
    Try {
      val body = RequestBody.create(jsonRequest, MediaType.parse("application/json; charset=utf-8"))
      val fullUrl = s"${config.hostPort}$path"
      new Request.Builder().url(fullUrl).put(body).addHeader("Content-Type", "application/json").build()
    } match {
      case Success(request) => Some(request)
      case Failure(exception) =>
        log.error(s"Failed to create PUT request due to ${exception.getMessage}")
        exception.printStackTrace()
        None
    }
  }



  override protected def send(data: String): Unit = {
    if (data.startsWith("ExecutionPlan")) {
      val jsonData = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
      val (sourceset, target) = getLineage(jsonData)
      if (sourceset.nonEmpty && target._1.nonEmpty) {
        makeLineage(sourceset, target)
      } else {
        logWarning("跳过空血缘关系")
      }
    }
  }

  def getLineage(jsonData: String): (mutable.LinkedHashSet[(String, String, String)], (String, String, String)) = {
    try {
      logInfo(s"开始解析血缘数据，数据长度: ${jsonData.length}")
      logInfo(s"血缘数据: ${jsonData}")
      logDebug(s"原始血缘数据: $jsonData")

      val operations = JSON.parseObject(jsonData).getJSONObject("operations")
      val readsArray = operations.getJSONArray("reads")
      val write = operations.getJSONObject("write")

      val targetType = getStringValue(write, "extra.destinationType")
      val targetDatabase = getStringValue(write, "params.table.identifier.database")
      val targetTableName = getStringValue(write, "params.table.identifier.table")

      logInfo(s"目标信息 - 类型: '$targetType', 数据库: '$targetDatabase', 表: '$targetTableName'")

  }

  def makeLineage(sourceset: mutable.LinkedHashSet[(String,String,String)], targeTuple: (String,String,String)): Unit = {
    //由于不支持一个json多个血缘,所以要单独拼接
    val targetTable = targeTuple._1
    val targetId = targeTuple._2
    val targetFqn = targeTuple._3
    for (sourceTuple <- sourceset) {
      val sourceTable = sourceTuple._1
      val sourceId = sourceTuple._2
      val sourceFqn = sourceTuple._3
      val stringlineage =
        s"""
           |{
           |    "edge": {
           |        "description": "string",
           |        "fromEntity": {
           |                "id": "$sourceId",
           |                "name": "$sourceTable",
           |                "fullyQualifiedName": "$sourceFqn",
           |                "deleted": false,
           |                "description": "a entity",
           |                "displayName": "$sourceTable",
           |                "href": "${config.hostPort}/api//v1/tables/$sourceId",
           |                "inherited": true,
           |                "type": "table"
           |            },
           |        "toEntity": {
           |            "id": "$targetId",
           |            "name": "$targetTable",
           |            "fullyQualifiedName": "$targetFqn",
           |            "deleted": false,
           |            "description": "a entity",
           |            "displayName": "$targetTable",
           |            "href": "${config.hostPort}/api//v1/tables/$targetId",
           |            "inherited": true,
           |            "type": "table"
           |        }
           |    }
           |}
           |""".stripMargin
    }
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

  def getMetadataTables(serviceType: String, database: String, tableName: String): (String, String, String) = {
    // 验证输入参数
    if (serviceType.isEmpty) {
      logWarning("服务类型为空，无法构建数据库schema")
      return ("", "", "")
    }
    if (database.isEmpty) {
      logWarning("数据库名为空，无法构建数据库schema")
      return ("", "", "")
    }
    if (tableName.isEmpty) {
      logWarning("表名为空，无法查询表信息")
      return ("", "", "")
    }
    
    val databaseSchema = serviceType match {
      case "hive" => 
        logDebug(s"使用Hive服务名: ${config.databaseServiceNames}")
        s"${config.databaseServiceNames}.default.$database"
      case "doris" => 
        logDebug(s"使用Doris服务名: ${config.databaseServiceNames}")
        s"${config.databaseServiceNames}.default.$database"
      case _ => 
        logWarning(s"未知的服务类型: $serviceType，使用默认格式")
        s"$serviceType.$database"
    }
    
    val cacheKey = s"$databaseSchema.$tableName"
    logInfo(s"查询表元数据 - 服务类型: $serviceType, 缓存键: $cacheKey")
    
    try {
      tableCache.getOrElseUpdate(cacheKey, {
        val url = s"${config.hostPort}/api/v1/tables/name/$cacheKey"
        logDebug(s"调用OpenMetadata API: $url")
        val tableMap = handleHttpGet(url)
        if (tableMap.nonEmpty) {
          logDebug(s"成功获取表信息: ${tableMap.keys.mkString(", ")}")
        } else {
          logWarning(s"API返回空结果: $url")
        }
        tableMap
      }).get(tableName) match {
        case Some(info) => 
          val result = (tableName, info("id"), info("fullyQualifiedName"))
          logInfo(s"成功获取表元数据: $result")
          result
        case None =>
          logWarning(s"在 $databaseSchema 中未找到表: $tableName")
          logDebug(s"缓存中的表列表: ${tableCache.get(cacheKey).map(_.keys.mkString(", ")).getOrElse("无")}")
          ("", "", "")
      }
    } catch {
      case e: Exception =>
        logError(s"获取表元数据时发生异常: ${e.getMessage}")
        logError(s"服务类型: $serviceType, 数据库: $database, 表名: $tableName")
        e.printStackTrace()
        ("", "", "")
    }
  }

  private val client = new OkHttpClient.Builder()
    .connectTimeout(HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    .readTimeout(HTTP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
    .build()

  def handleHttpGet(url: String): Map[String, Map[String, String]] = {
    var response: Response = null
    try {
      val request = new Request.Builder()
        .url(url)
        .addHeader("Content-Type", CONTENT_TYPE_JSON)
        .addHeader("Authorization", s"Bearer ${config.token}")
        .build()

      response = client.newCall(request).execute()
      val responseCode = response.code()
      val responseBody = response.body().string()
      if (responseCode >= 200 && responseCode < 300) {
        val jsonResponse = JSON.parseObject(responseBody)
        val mainId = jsonResponse.getString("id")
        val mainFullyQualifiedName = jsonResponse.getString("fullyQualifiedName")
        val mainMap = Map("id" -> mainId, "fullyQualifiedName" -> mainFullyQualifiedName)
        val mainName = jsonResponse.getString("name")
        val result = Map(mainName -> mainMap)
        result
      } else {
        println("Response code not in success range")
        Map.empty[String, Map[String, String]]
      }
    } catch {
      case e: Exception =>
        println(s"Exception in handleHttpGet: ${e.getMessage}")
        e.printStackTrace()
        Map.empty[String, Map[String, String]]
    } finally {
      if (response != null) {
        response.close()
      }
    }
  }

  def handleHttpPut(jsonParam: String): Map[String, String] = {
    var response: Response = null
    val lineageUrl = s"${config.apiUrl}/api/v1/lineage"
  
    try {
      val mediaType = MediaType.parse(CONTENT_TYPE_JSON)
      val body = RequestBody.create(mediaType, jsonParam)
      val request = new Request.Builder()
        .url(lineageUrl)
        .put(body)
        .addHeader("Content-Type", CONTENT_TYPE_JSON)
        .addHeader("Authorization", s"Bearer ${config.token}")
        .build()
  
      response = client.newCall(request).execute()
      val responseCode = response.code()
      val responseBody = response.body().string()
  
      if (responseBody != null && responseBody.nonEmpty) {
        try {
          val dataArray = JSON.parseObject(responseBody).getJSONArray("data")
          dataArray.asScala.map { obj =>
            val jsonObj = obj.asInstanceOf[JSONObject]
            jsonObj.getString("name") -> jsonObj.getString("id")
          }.toMap
        } catch {
          case e: Exception =>
            println(s"解析JSON响应失败: ${e.getMessage}")
            Map.empty[String, String]
        }
      } else {
        println("发送成功 无返回值")
        Map.empty[String, String]
      }
    } catch {
      case e: Exception =>
        logError(s"HTTP PUT 请求异常: ${e.getMessage}", e)
        Map.empty[String, String]
    } finally {
      // 确保响应体被关闭
      if (response != null) {
        response.close()
      }
    }
  }
}
