
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
 * 
 */
package za.co.absa.spline.harvester.dispatcher.openmedataline
import org.apache.commons.configuration.Configuration
import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import scala.collection.mutable
import com.alibaba.fastjson2.{JSON, JSONObject}
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class OpenmetadataLineageDispatcher(val config: OpenmetadataLineageDispatcherConfig) extends AbstractJsonLineageDispatcher with Logging {

  def this(configuration: Configuration) = this(new OpenmetadataLineageDispatcherConfig(configuration))

  // 常量定义
  private val HTTP_TIMEOUT_SECONDS = 30
  private val CONTENT_TYPE_JSON = "application/json"
  private val tableCache = mutable.Map[String, Map[String, Map[String, String]]]()

  override def name = "Openmetadata"

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
      val operations = JSON.parseObject(jsonData).getJSONObject("operations")
      val readsArray = operations.getJSONArray("reads")
      val write = operations.getJSONObject("write")

      val targetType = getStringValue(write, "extra.destinationType")
      val targetDatabase = getStringValue(write, "params.table.identifier.database")
      val targetTableName = getStringValue(write, "params.table.identifier.table")

      val sourceset = mutable.LinkedHashSet[(String, String, String)]()
      
      for (i <- 0 until readsArray.size()) {
        val readObj = readsArray.getJSONObject(i)
        val sourceType = getStringValue(readObj, "extra.sourceType")
        val sourceTable = getStringValue(readObj, "params.table.identifier.table")
        val sourceDatabase = getStringValue(readObj, "params.table.identifier.database")
        
        if (sourceTable.nonEmpty && sourceDatabase.nonEmpty) {
          val sourceTuple = getMetadataTables(sourceType, sourceDatabase, sourceTable)
          sourceset.add(sourceTuple)
        }
      }
      
      val targetTuple = getMetadataTables(targetType, targetDatabase, targetTableName)
      (sourceset, targetTuple)
    } catch {
      case e: Exception =>
        logError(s"解析血缘数据失败: ${e.getMessage}")
        (mutable.LinkedHashSet.empty, ("", "", ""))
    }
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
           |                "href": "${config.apiUrl}/api//v1/tables/$sourceId",
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
           |            "href": "${config.apiUrl}/api//v1/tables/$targetId",
           |            "inherited": true,
           |            "type": "table"
           |        }
           |    }
           |}
           |""".stripMargin
      handleHttpPost(stringlineage)
    }
  }
  private def getStringValue(json: JSONObject, path: String): String = {
    try {
      val keys = path.split("\\.")
      val lastIndex = keys.length - 1
      val parentObj = keys.dropRight(1).foldLeft(json) { (obj, key) =>
        if (obj != null && obj.containsKey(key)) obj.getJSONObject(key) else null
      }
      if (parentObj != null && parentObj.containsKey(keys(lastIndex))) {
        parentObj.getString(keys(lastIndex))
      } else {
        ""
      }
    } catch {
      case _: Exception => ""
    }
  }

  def getMetadataTables(serviceType: String, database: String, tableName: String): (String, String, String) = {

    val databaseSchema = serviceType match {
      case "hive" => s"${config.servicename}.default.$database"
      case _ => s"$serviceType.$database"
    }

    val cacheKey = s"$databaseSchema.$tableName"
    
    tableCache.getOrElseUpdate(cacheKey, {
      val url = s"${config.apiUrl}/api/v1/tables?databaseSchema=$databaseSchema"
      val tableMap = handleHttpGet(url)
      tableMap
    }).get(tableName) match {
      case Some(info) => (tableName, info("id"), info("fullyQualifiedName"))
      case None => logWarning(s"在 $databaseSchema 中未找到表: $tableName")
        ("", "", "")
    }
  }

  def handleHttpGet(url: String): Map[String, Map[String, String]] = {
    try {
      val connection = new java.net.URL(url).openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("GET")
      connection.setRequestProperty("Content-Type", CONTENT_TYPE_JSON)
      connection.setRequestProperty("Authorization", s"Bearer ${config.token}")
      connection.setConnectTimeout(HTTP_TIMEOUT_SECONDS * 1000)
      connection.setReadTimeout(HTTP_TIMEOUT_SECONDS * 1000)
      
      val responseCode = connection.getResponseCode
      val responseBody = if (responseCode >= 200 && responseCode < 300) {
        scala.io.Source.fromInputStream(connection.getInputStream).mkString
      } else {
        scala.io.Source.fromInputStream(connection.getErrorStream).mkString
      }

      if (responseCode >= 200 && responseCode < 300) {
        val dataArray = JSON.parseObject(responseBody).getJSONArray("data")
        dataArray.asScala.map { obj =>
          val jsonObj = obj.asInstanceOf[JSONObject]
          jsonObj.getString("name") -> Map(
            "id" -> jsonObj.getString("id"),
            "fullyQualifiedName" -> jsonObj.getString("fullyQualifiedName")
          )
        }.toMap
      } else {
        logError(s"HTTP GET 请求失败: $responseCode, URL: $url, 响应: $responseBody")
        Map.empty[String, Map[String, String]]
      }
    } catch {
      case e: Exception =>
        logError(s"HTTP GET 请求异常: ${e.getMessage}", e)
        Map.empty[String, Map[String, String]]
    }
  }

  def handleHttpPost(jsonParam: String): Map[String, String] = {
    val lineageUrl = s"${config.apiUrl}/api/v1/lineage"
    
    try {
      val connection = new java.net.URL(lineageUrl).openConnection().asInstanceOf[java.net.HttpURLConnection]
      connection.setRequestMethod("PUT")
      connection.setRequestProperty("Content-Type", CONTENT_TYPE_JSON)
      connection.setRequestProperty("Authorization", s"Bearer ${config.token}")
      connection.setConnectTimeout(HTTP_TIMEOUT_SECONDS * 1000)
      connection.setReadTimeout(HTTP_TIMEOUT_SECONDS * 1000)
      connection.setDoOutput(true)

      val outputStream = connection.getOutputStream
      outputStream.write(jsonParam.getBytes("UTF-8"))
      outputStream.close()

      val responseCode = connection.getResponseCode
      val responseBody = if (responseCode >= 200 && responseCode < 300) {
        scala.io.Source.fromInputStream(connection.getInputStream).mkString
      } else {
        scala.io.Source.fromInputStream(connection.getErrorStream).mkString
      }

      if (responseCode >= 200 && responseCode < 300) {
        val dataArray = JSON.parseObject(responseBody).getJSONArray("data")
        dataArray.asScala.map { obj =>
          val jsonObj = obj.asInstanceOf[JSONObject]
          jsonObj.getString("name") -> jsonObj.getString("id")
        }.toMap
      } else {
        logError(s"HTTP PUT 请求失败: $responseCode, URL: $lineageUrl, 响应: $responseBody")
        Map.empty[String, String]
      }
    } catch {
      case e: Exception =>
        logError(s"HTTP PUT 请求异常: ${e.getMessage}", e)
        Map.empty[String, String]
    }
  }
}
