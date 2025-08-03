/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.spline.harvester.plugin.embedded

import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JDBCRelation}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.{AccessorMethodValueExtractor, SafeTypeMatchingExtractor}
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DorisPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin, RelationProviderProcessing}

import javax.annotation.Priority
import org.slf4j.LoggerFactory

import scala.util.matching.Regex
import scala.util.Try
import scala.util.matching.compat.RegexOps

@Priority(Precedence.Normal)
class DorisPlugin
  extends Plugin
    with RelationProviderProcessing
    with BaseRelationProcessing {

  private val log = LoggerFactory.getLogger(classOf[DorisPlugin])

  // 预编译正则表达式以提高性能
  private val DorisJdbcUrlPattern = "^jdbc:mysql://.*(?:doris|DORIS).*$".r
  private val PortPattern = ":(9030|9031|8030|8031)(?:/|$)".r
  private val VersionPattern = DorisPlugin.DorisVersionPattern


  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if isDorisProvider(rp) =>
      val config = try {
        DorisConfig.fromOptions(cmd.options)
      } catch {
        case e: IllegalArgumentException =>
          log.warn(s"Missing required parameters: ${e.getMessage}, using defaults")
          DorisConfig.fromOptions(cmd.options + ("fenodes" -> "localhost:8030", "table" -> "unknown_table"))
      }
      
      val enhancedParams = config.toMap ++ Map(
        "sourceType" -> "doris",
        "operationType" -> "write",
        "pluginVersion" -> "1.0.0"
      )

      WriteNodeInfo(
        asDorisSourceId(config.fenodes, config.database, config.table),
        cmd.mode,
        cmd.query,
        enhancedParams
      )
  }

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: DorisRelation`(relation), _) =>
      val params = extractValue[Map[String, String]](relation, "parameters")
      val config = try {
        DorisConfig.fromOptions(params)
      } catch {
        case e: IllegalArgumentException =>
          log.warn(s"Missing required parameters: ${e.getMessage}, using defaults")
          DorisConfig.fromOptions(params + ("fenodes" -> "localhost:8030", "table" -> "unknown_table"))
      }
      
      val enhancedParams = config.toMap ++ Map(
        "sourceType" -> "doris",
        "operationType" -> "read",
        "pluginVersion" -> "1.0.0"
      )

      ReadNodeInfo(
        asDorisSourceId(config.fenodes, config.database, config.table),
        enhancedParams
      )

    case (`_: JDBCRelation`(jr), _) =>
      val jdbcOptions = extractValue[JDBCOptions](jr, "jdbcOptions")
      val url = extractValue[String](jdbcOptions, "url")
      
      if (isDorisJdbcUrl(url)) {
        val TableOrQueryFromJDBCOptionsExtractor(table) = jdbcOptions
        val config = try {
          DorisConfig.fromJdbcUrl(url, table)
        } catch {
          case e: Exception =>
          log.warn(s"Failed to parse JDBC URL: ${e.getMessage}, using defaults")
            DorisConfig.fromJdbcUrl(url, table)
        }
        
        val dorisVersion = detectDorisVersion(url)
        val enhancedParams = config.toMap ++ Map(
          "sourceType" -> "doris",
          "connectionType" -> "jdbc"
        ) ++ dorisVersion.map("dorisVersion" -> _).toMap

        ReadNodeInfo(
          asDorisSourceId(config.fenodes, config.database, config.table),
          enhancedParams
        )
      } else {
        throw new MatchError("Not a Doris JDBC connection")
      }
  }

  private def asDorisSourceId(fenodes: String, database: String, table: String): SourceIdentifier = {
    val cleanFenodes = if (fenodes.trim.isEmpty) "localhost:8030" else fenodes.trim
    val cleanDatabase = if (database.trim.isEmpty) "default" else database.trim
    val cleanTable = if (table.trim.isEmpty) "unknown_table" else table.trim
    
    // 处理多个FE节点的情况，移除空格并标准化
    val normalizedFenodes = cleanFenodes.split(",").map(_.trim).filter(_.nonEmpty).mkString(",")
    
    SourceIdentifier(
      Some("doris"), 
      s"doris://$normalizedFenodes/$cleanDatabase.$cleanTable"
    )
  }

  private def isDorisProvider(rp: AnyRef): Boolean = {
    val rpStr = rp.toString.toLowerCase
    rpStr == "doris" || 
    rpStr.contains("org.apache.doris.spark") || 
    rpStr.contains("doris.spark.sql") ||
    rpStr.endsWith(".doris")
  }

  private def isDorisJdbcUrl(url: String): Boolean = {
    url.startsWith("jdbc:mysql://") && (
      DorisJdbcUrlPattern.matches(url) ||
      PortPattern.findFirstIn(url).isDefined
    )
  }

  private def detectDorisVersion(url: String): Option[String] = {
    VersionPattern.findFirstMatchIn(url.toLowerCase)
      .map(_.group(1))
      .orElse(extractVersionFromJdbcMetadata(url))
  }

  private def extractVersionFromJdbcMetadata(url: String): Option[String] = {
    // 通过JDBC连接获取数据库版本的预留接口
    None
  }
}

object DorisPlugin {
  // 共享正则表达式模式
  val FenodesPattern = "^([^:]+:\\d+(?:,[^:]+:\\d+)*)$".r
  val TablePattern = "^(?:(\\w+)\\.)?(\\w+)$".r
  val DorisVersionPattern = ".*doris.*?(\\d+\\.\\d+\\.\\d+).*$".r
  
  private object `_: DorisRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.doris.spark.sql.DorisRelation")
  private object `_: JDBCRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation")
  
  private object TableOrQueryFromJDBCOptionsExtractor extends AccessorMethodValueExtractor[String]("table", "tableOrQuery")

  case class DorisConfig(
    fenodes: String,
    database: String,
    table: String,
    user: Option[String] = None,
    password: Option[String] = None,
    additionalParams: Map[String, String] = Map.empty,
    queryTimeout: Option[String] = None,
    connectionPoolSize: Option[String] = None
  ) {
    def toMap: Map[String, String] = {
      Map(
        "fenodes" -> fenodes,
        "database" -> database,
        "table" -> table
      ) ++ 
      user.map("user" -> _).toMap ++
      password.map("password" -> _).toMap ++
      queryTimeout.map("queryTimeout" -> _).toMap ++
      connectionPoolSize.map("connectionPoolSize" -> _).toMap ++
      additionalParams
    }
  }

  object DorisConfig {
    // 已在主类中预编译

    def fromOptions(options: Map[String, String]): DorisConfig = {
      val fenodes = options.get("fenodes")
        .orElse(options.get("doris.fenodes"))
        .orElse(options.get("doris.host"))
        .getOrElse(throw new IllegalArgumentException("Missing required parameter: fenodes"))

      val database = options.getOrElse("database", 
        options.getOrElse("doris.database", ""))
      
      val table = options.get("table")
        .orElse(options.get("dbtable"))
        .orElse(options.get("doris.table.identifier"))
        .orElse(options.get("doris.table"))
        .getOrElse(throw new IllegalArgumentException("Missing required parameter: table"))

      val user = options.get("doris.user").orElse(options.get("user"))
      val password = options.get("doris.password").orElse(options.get("password"))
      val queryTimeout = options.get("doris.query.timeout")
      val connectionPoolSize = options.get("doris.connection.pool.size")

      val additionalParams = options.filterKeys(key => 
        key.startsWith("doris.") && !Set("doris.fenodes", "doris.table.identifier", "doris.table", "doris.user", "doris.password", "doris.database", "doris.host").contains(key)
      )

      DorisConfig(fenodes, database, table, user, password, additionalParams, queryTimeout, connectionPoolSize)
    }

    def fromJdbcUrl(url: String, tableName: String): DorisConfig = {
      val endpoint = extractDorisEndpoints(url)
      val (database, table) = parseTableName(tableName)
      
      val user = extractUserFromUrl(url)
      val password = extractPasswordFromUrl(url)

      DorisConfig(endpoint, database, table, user, password)
    }

    private def extractDorisEndpoints(url: String): String = {
      val pattern = "jdbc:mysql://([^/]+)/.*".r
      url match {
        case pattern(endpoint) => endpoint
        case _ => {
          log.warn(s"Failed to extract Doris endpoints from URL: $url, using default")
          "localhost:9030"
        }
      }
    }

    private def parseTableName(fullTableName: String): (String, String) = {
      fullTableName match {
        case TablePattern(db, tbl) => (db, tbl)
        case _ => ("", fullTableName)
      }
    }

    private def extractUserFromUrl(url: String): Option[String] = {
      val pattern = """jdbc:mysql://([^:]+):[^@]*@""".r
      url match {
        case pattern(user) => Some(user)
        case _ => None
      }
    }

    private def extractPasswordFromUrl(url: String): Option[String] = {
    val pattern = """jdbc:mysql://[^:]*:([^@]+)@""".r
    url match {
      case pattern(password) => Some(maskPassword(password))
      case _ => None
    }
  }

  private def maskPassword(password: String): String = {
    if (password.length <= 2) "*" * password.length
    else password.head + "*" * (password.length - 2) + password.last
  }
    
    private def detectDorisVersion(url: String): Option[String] = {
      url match {
        case DorisPlugin.DorisVersionPattern(version) => Some(version)
        case _ => None
      }
    }
  }
}
