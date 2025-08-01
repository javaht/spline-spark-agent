
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
import okhttp3.{MediaType, OkHttpClient, Request, Response}
import okhttp3.RequestBody
import org.apache.spark.internal.Logging
import scala.collection.mutable
import com.alibaba.fastjson2.{JSON, JSONArray, JSONObject}
import za.co.absa.spline.harvester.dispatcher.AbstractJsonLineageDispatcher

import scala.collection.JavaConverters._

class OpenmetadataLineageDispatcher(val config: OpenmetadataLineageDispatcherConfig) extends AbstractJsonLineageDispatcher  with Logging {

  def this(configuration: Configuration) = this(new OpenmetadataLineageDispatcherConfig(configuration))

  override def name = "Openmetadata"

  override protected def send(data: String): Unit = {
    if (data.startsWith("ExecutionPlan")) {
      val replaceDate = StringUtils.replace(data, "ExecutionPlan (apiVersion: 1.2):", "")
      val tuple = getLineage(replaceDate) //这里拼接的血缘
      if(){

      }
      makeLineage(tuple._1, tuple._2)
    }
  }

  def getLineage(replaceDate: String): (mutable.LinkedHashSet[(String,String,String)],(String,String,String)) = {
    val operations: JSONObject  = JSON.parseObject(replaceDate).getJSONObject("operations")

    val readsArray: JSONArray = operations.getJSONArray("reads") //这里开始获取数据来源
    val write: JSONObject = operations.getJSONObject("write")

    val targetype: String = write.getJSONObject("extra").getString("destinationType") //目标数据源类型
    val targetDatabase: String = write.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("database") //目标数据库
    val targetTablename: String = write.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("table")   //目标表

    val sourset = new mutable.LinkedHashSet[(String,String,String)]
    for (i <- 0 until readsArray.size) {
      val readObj: JSONObject = readsArray.getJSONObject(i)
      val sourceType = readObj.getJSONObject("extra").getString("sourceType")
      val sourceTable: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("table")
      val sourceDatabase: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("database")
      val sourceTuple = getmetadataTables(sourceType, sourceDatabase, sourceTable)
      sourset.add(sourceTuple)
    }
    val targeTuple = getmetadataTables(targetype,targetDatabase, targetTablename)
    (sourset,targeTuple)
  }

  def makeLineage(sourset: mutable.LinkedHashSet[(String,String,String)], targeTuple: (String,String,String)): Unit = {
    //由于不支持一个json多个血缘,所以要单独拼接
    val targetTable = targeTuple._1
    val targetId = targeTuple._2
    val targetFqn = targeTuple._3
    for (sourceTuple <- sourset) {
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

  def getmetadataTables(servicetype: String,database: String, tablename: String): (String,String, String) = {
    var  databases=""
    if(servicetype.equals("hive")){
      databases=  "hive.default".concat(database)
    }

    val url = s"${config.apiUrl}/api/v1/tables?databaseSchema=$databases"
    val kvMap = handleHttpGet(url)
    if (kvMap.contains(tablename)){
      (tablename,kvMap(tablename).get("id").toString,kvMap(tablename).get("fullyQualifiedName").toString)
    }else{
      logInfo(s"未在$databases 中找到表$tablename")
      ("","","")
    }
  }

  def handleHttpGet(url: String): Map[String, Map[String,String]] = {
    try {
      val client = new OkHttpClient()
      val request = new Request.Builder()
        .url(url)
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + config.token)
        .get()
        .build()

      val response: Response = client.newCall(request).execute()
      val code = response.code()
      val responseBody = if (response.body() != null) response.body().string() else ""

      if (code == 200) {
        val dataArray = JSON.parseObject(responseBody).getJSONArray("data")

        val kvMap = dataArray.asScala.map { obj =>
          val jsonObj = obj.asInstanceOf[com.alibaba.fastjson2.JSONObject]
          jsonObj.getString("name") -> Map(
            "id" -> jsonObj.getString("id"),
            "fullyQualifiedName" -> jsonObj.getString("fullyQualifiedName")
          )
        }.toMap
        kvMap
      } else {
        System.out.println("请求失败,code=" + code + "," + responseBody + ",url=" + url)
        Map.empty[String, Map[String,String]]
      }
    } catch {
      case e: Exception =>
        System.out.println("接口调用出现异常……")
        e.printStackTrace()
        Map.empty[String, Map[String,String]]
    }
  }

  def handleHttpPost(jsonParam: String): Map[String, String] = {
    val lineageUrl = s"${config.apiUrl}/api/v1/lineage"
    try {
      val client = new OkHttpClient()
      val jsonMediaType = MediaType.parse("application/json; charset=utf-8")
      val body = RequestBody.create(jsonMediaType, jsonParam)

      val request = new Request.Builder()
        .url(lineageUrl)
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "Bearer " + config.token)
        .post(body)
        .build()

      val response: Response = client.newCall(request).execute()
      val code = response.code()
      val responseBody = if (response.body() != null) response.body().string() else ""

      if (code == 200) {
        val dataArray = JSON.parseObject(responseBody).getJSONArray("data")

        val kvMap = dataArray.asScala.map { obj =>
          val jsonObj = obj.asInstanceOf[com.alibaba.fastjson2.JSONObject]
          jsonObj.getString("name") -> jsonObj.getString("id")
        }.toMap
        kvMap
      } else {
        System.out.println("请求失败,code=" + code + "," + responseBody + ",url=" + lineageUrl)
        Map.empty[String, String]
      }
    } catch {
      case e: Exception =>
        System.out.println("接口调用出现异常……")
        e.printStackTrace()
        Map.empty[String, String]
    }
  }
}
