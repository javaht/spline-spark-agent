package za.co.absa.spline.harvester.dispatcher.openmedatalineage

import com.alibaba.fastjson2.{JSON, JSONArray, JSONObject}
import scalaj.http.{Http, HttpRequest}

import java.net.URI
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._
object test {

  def main(args: Array[String]): Unit = {
    val result = getEntity()
    println(s"Result: $result")
  }
  private def getEntity(): Map[String, JSONObject] = {
    Try {
      val request = createGetTableRequest("ads_media_account_info", "default", Some("hive"))
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
          fromEntityJson.put("href", s"http://172.16.0.179:8585/api/v1/tables/$tableId": Object)
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
  



  private def sendRequest(request: HttpRequest): Map[String, Any] = {
    Try {
      val response = request.header("Authorization", s"Bearer eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTM2OTUwNTYsImV4cCI6bnVsbH0.N4hARFRrfI06NVZGJIYg_bEg2WY-Z4AoITdVScstxG0NEcU_17zsP1yyO05OqH867QeEqKczu1pZ4XUU1DmR7INxhd3gyF5peO94K8tRjpWcOIxQQdyTGPRy_SfBapRiNhND5OHEAk2aq_z4mBKmnP83Kwq0jwdKhE9xz7_PFtRhGN1vdEzPuOL-6A-WKjh7Y3ixyHqOdbyPfm-XDth2yPShqJ_gNArvWhBkOZxvbpylE6eOFDj__woChwB6dtpYAVwXHP7MGxTLUzBiyc8YQojfmO1hSRIu6hSXwntBqQlzyiyohwjmR3O6tgJrEkYou59tkfW_BuYo6jo_QJtDqQ").asString
      if (response.isSuccess) {
        // 直接使用JSON字符串解析，避免JSONObject类型转换问题
        parseJsonToMap(response.body)
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
  
  // 辅助方法：将JSON字符串转换为Scala Map
  private def parseJsonToMap(jsonString: String): Map[String, Any] = {
    val jsonObj = JSON.parseObject(jsonString)
    convertJsonObjectToMap(jsonObj)
  }
  
  // 递归转换JSONObject为Scala Map
  private def convertJsonObjectToMap(jsonObj: com.alibaba.fastjson2.JSONObject): Map[String, Any] = {
    jsonObj.entrySet().asScala.map { entry =>
      val key = entry.getKey
      val value = entry.getValue match {
        case obj: com.alibaba.fastjson2.JSONObject => convertJsonObjectToMap(obj)
        case arr: com.alibaba.fastjson2.JSONArray => 
          arr.asScala.map {
            case obj: com.alibaba.fastjson2.JSONObject => convertJsonObjectToMap(obj)
            case other => other
          }.toList
        case other => other
      }
      key -> value
    }.toMap
  }

  def createGetTableRequest(tableName: String, databaseName:String,dbServiceName: Option[String] = None): HttpRequest = {
    val fqnQuery = dbServiceName match {
      case Some(service) => s"$service.*$databaseName.*$tableName"
      case None => s"*$tableName"
    }
    val path = "api/v1/search/fieldQuery"
    val queryParams = Map(
      "size" -> "10",
      "fieldName" -> "fullyQualifiedName",
      "fieldValue" -> fqnQuery,
      "from" -> "0",
      "index" -> "table_search_index"
    )
    createHttpRequest(path, queryParams)
  }


  private def createHttpRequest(path: String, queryParams: Map[String, String]): HttpRequest = {
    val baseUri = new URI("http://172.16.0.179:8585")
    val fullUrl = s"${baseUri.getScheme}://${baseUri.getHost}:${baseUri.getPort}/$path"
    val request = Http(fullUrl).params(queryParams).header("Accept", "application/json").header("Content-Type", "application/json")
    request
  }


}
