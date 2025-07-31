package za.co.absa.spline.harvester.dispatcher

import org.openmetadata.client.ApiClient
import org.openmetadata.client.api.DashboardsApi
import org.openmetadata.client.api.LineageApi
import org.openmetadata.client.gateway.OpenMetadata
import org.openmetadata.client.model.AddLineage
import org.openmetadata.client.model.EntitiesEdge
import org.openmetadata.client.model.EntityReference
import org.openmetadata.client.model.LineageDetails
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig
import org.openmetadata.schema.services.connections.metadata.AuthProvider
import org.openmetadata.schema.services.connections.metadata.OpenMetadataConnection
import org.openmetadata.schema.entity._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable

class OpenmetadaLineageDispatcher extends AbstractJsonLineageDispatcher {

  override def name = "OpenMetada"
  override protected def send(exeplan: String): Unit = {
    val source = makeLine(exeplan)._1
    val target = makeLine(exeplan)._2



  }


  def getIdByfqn(): Unit = {




  }

  def makeLine(exeplan: String): (String,String) = {


    val operations: JSONObject  = JSON.parseObject(exeplan).getJSONObject("operations");

    val write: JSONObject = operations.getJSONObject("write")  //这里开始获取数据写入源
    val readsArray: JSONArray = operations.getJSONArray("reads") //这里开始获取数据来源
    val `type`: String = write.getJSONObject("extra").getString("destinationType") //写入类型

    val params: JSONObject = write.getJSONObject("params")

    val targetDatabase: String = params.getString("database")//目标数据库
    val targetTablename: String = params.getString("table") //目标表

    val targetset: String = targetDatabase+"."+targetTablename

    val sourset: mutable.LinkedHashSet[String] = new mutable.LinkedHashSet[String] //定义一个不允许重复的集合
    for (i <- 0 until readsArray.size) {
      val readObj: JSONObject = readsArray.getJSONObject(i)
      val sourceTable: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("table")
      val sourceDatabase: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("database")
      sourset.add(sourceDatabase + "." + sourceTable)
    }

     (String.valueOf(sourset),targetset)
  }


}
