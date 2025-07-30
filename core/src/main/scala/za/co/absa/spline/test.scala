package za.co.absa.spline

import org.openmetadata.client.api.{DatabasesApi, LineageApi}
import org.openmetadata.client.gateway.OpenMetadata
import org.openmetadata.schema.security.client.OpenMetadataJWTClientConfig
import org.openmetadata.schema.services.connections.metadata.{AuthProvider, OpenMetadataConnection}

object test {
  def main(args: Array[String]): Unit = {




    val openMetadataJWTClientConfig  = new OpenMetadataJWTClientConfig()
    openMetadataJWTClientConfig.setJwtToken("eyJraWQiOiJHYjM4OWEtOWY3Ni1nZGpzLWE5MmotMDI0MmJrOTQzNTYiLCJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJvcGVuLW1ldGFkYXRhLm9yZyIsInN1YiI6ImxpbmVhZ2UtYm90Iiwicm9sZXMiOlsiTGluZWFnZUJvdFJvbGUiXSwiZW1haWwiOiJsaW5lYWdlLWJvdEBvcGVuLW1ldGFkYXRhLm9yZyIsImlzQm90Ijp0cnVlLCJ0b2tlblR5cGUiOiJCT1QiLCJpYXQiOjE3NTM2OTUwNTYsImV4cCI6bnVsbH0.N4hARFRrfI06NVZGJIYg_bEg2WY-Z4AoITdVScstxG0NEcU_17zsP1yyO05OqH867QeEqKczu1pZ4XUU1DmR7INxhd3gyF5peO94K8tRjpWcOIxQQdyTGPRy_SfBapRiNhND5OHEAk2aq_z4mBKmnP83Kwq0jwdKhE9xz7_PFtRhGN1vdEzPuOL-6A-WKjh7Y3ixyHqOdbyPfm-XDth2yPShqJ_gNArvWhBkOZxvbpylE6eOFDj__woChwB6dtpYAVwXHP7MGxTLUzBiyc8YQojfmO1hSRIu6hSXwntBqQlzyiyohwjmR3O6tgJrEkYou59tkfW_BuYo6jo_QJtDqQ")
    val openMetadataConnection = new OpenMetadataConnection()
    openMetadataConnection.setHostPort("http://172.16.0.201:8585/api");
    openMetadataConnection.setAuthProvider(AuthProvider.OPENMETADATA);
    openMetadataConnection.setSecurityConfig(openMetadataJWTClientConfig);
    val openMetadataGateway = new OpenMetadata(openMetadataConnection);
    val lineageApi = openMetadataGateway.buildClient(classOf[LineageApi])

    val DatabasesApi = openMetadataGateway.buildClient(classOf[DatabasesApi])

    DatabasesApi.get






  }

}
