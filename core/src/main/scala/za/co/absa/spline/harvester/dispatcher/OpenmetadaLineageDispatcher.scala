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








    val openMetadataJWTClientConfig  = new OpenMetadataJWTClientConfig()
    openMetadataJWTClientConfig.setJwtToken("eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImlzQm90IjpmYWxzZSwiaXNzIjoib3Blbi1tZXRhZGF0YS5vcmciLCJpYXQiOjE2NjM5Mzg0NjIsImVtYWlsIjoiYWRtaW5Ab3Blbm1ldGFkYXRhLm9yZyJ9.tS8um_5DKu7HgzGBzS1VTA5uUjKWOCU0B_j08WXBiEC0mr0zNREkqVfwFDD-d24HlNEbrqioLsBuFRiwIWKc1m_ZlVQbG7P36RUxhuv2vbSp80FKyNM-Tj93FDzq91jsyNmsQhyNv_fNr3TXfzzSPjHt8Go0FMMP66weoKMgW2PbXlhVKwEuXUHyakLLzewm9UMeQaEiRzhiTMU3UkLXcKbYEJJvfNFcLwSl9W8JCO_l0Yj3ud-qt_nQYEZwqW6u5nfdQllN133iikV4fM5QZsMCnm8Rq1mvLR0y9bmJiD7fwM1tmJ791TUWqmKaTnP49U493VanKpUAfzIiOiIbhg")
    val openMetadataConnection = new OpenMetadataConnection()
    openMetadataConnection.setHostPort("http://localhost:8585/api");
    openMetadataConnection.setAuthProvider(AuthProvider.OPENMETADATA);
    openMetadataConnection.setSecurityConfig(openMetadataJWTClientConfig);
    val openMetadataGateway = new OpenMetadata(openMetadataConnection);
    val lineageApi = openMetadataGateway.buildClient(classOf[LineageApi])



    // 构建源实体引用
    val fromEntity = new EntityReference();
    fromEntity.setId(UUID.fromString("source-entity-id"));
    fromEntity.setType("table");
    fromEntity.setName("")

    // 构建目标实体引用
    val toEntity = new EntityReference();
    toEntity.setId(UUID.fromString("target-entity-id"));
    toEntity.setType("table");

    // 构建血缘关系详情
    val lineageDetails = new LineageDetails();
    lineageDetails.setCreatedAt(System.currentTimeMillis());
    lineageDetails.setCreatedBy("user");
    lineageDetails.setUpdatedAt(System.currentTimeMillis());
    lineageDetails.setUpdatedBy("user");

    // 构建 EntitiesEdge 对象
    val entitiesEdge = new EntitiesEdge();
    entitiesEdge.setFromEntity(fromEntity);
    entitiesEdge.setToEntity(toEntity);
    entitiesEdge.setLineageDetails(lineageDetails);

    // 构建 AddLineage 对象
    val addLineage = new AddLineage();
    addLineage.setEdge(entitiesEdge);

    lineageApi.addLineageEdge(addLineage);


  }


  def makeLine(exeplan: String): Unit = {
    var downstreamUrn: String = ""

    val operations: JSONObject  = JSON.parseObject(exeplan).getJSONObject("operations");

    val write: JSONObject = operations.getJSONObject("write")  //这里开始获取数据写入源
    val readsArray: JSONArray = operations.getJSONArray("reads") //这里开始获取数据来源
    val `type`: String = write.getJSONObject("extra").getString("destinationType") //写入类型

    val params: JSONObject = write.getJSONObject("params")

    val targetDatabase: String = params.getString("hoodie.datasource.hive_sync.database")//目标数据库
    val targetTablename: String = params.getString("hoodie.datasource.hive_sync.table") //目标表

    val sourset: mutable.LinkedHashSet[String] = new mutable.LinkedHashSet[String] //定义一个不允许重复的集合
    for (i <- 0 until readsArray.size) {
      val readObj: JSONObject = readsArray.getJSONObject(i)
      val sourceTable: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("table")
      val sourceDatabase: String = readObj.getJSONObject("params").getJSONObject("table").getJSONObject("identifier").getString("database")
      sourset.add(sourceDatabase + "." + sourceTable)
    }





    val jsonParamList = mutable.LinkedHashSet[String]();



  }


}
