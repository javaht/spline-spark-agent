/*
 * Copyright 2024 ABSA Group Limited
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

import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SaveMode
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DorisPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, DataSourceFormatNameResolving, Plugin, RelationProviderProcessing, WriteNodeProcessing, ReadNodeProcessing}
import za.co.absa.spline.agent.SplineAgent
import javax.annotation.Priority
import scala.util.{Try}



@Priority(Precedence.Highest)
class DorisPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing
    with DataSourceFormatNameResolving
    with WriteNodeProcessing
    with ReadNodeProcessing {

  import org.slf4j.LoggerFactory
  private val log = LoggerFactory.getLogger(classOf[DorisPlugin])

  /**
   * 处理传统的Doris读操作
   */
  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: DorisRelation`(dorisRelation), _) =>
      Try {
        val database = extractValue[String](dorisRelation, "database")
        val table = extractValue[String](dorisRelation, "table")
        val params = extractConnectionParams(dorisRelation) ++ createTableIdentifier(database, table)
        ReadNodeInfo(DorisPlugin.asSourceId(database, table), params)
      }.recover {
        case ex =>
          log.warn(s"Failed to extract Doris read metadata: ${ex.getMessage}")
          ReadNodeInfo(DorisPlugin.asSourceId("unknown", "unknown"), Map("error" -> ex.getMessage))
      }.get
  }

  /**
   * 处理传统的Doris写操作
   */
  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if isDorisProvider(rp) =>
      Try {
        val database = extractDatabaseFromOptions(cmd.options)
        val table = extractTableFromOptions(cmd.options)
        val fenodes = extractFenodesFromOptions(cmd.options)
        val enhancedOptions = cmd.options ++ createTableIdentifier(database, table)
        WriteNodeInfo(DorisPlugin.asSourceIdWithFenodes(fenodes, database, table), cmd.mode, cmd.query, enhancedOptions)
      }.recover {
        case ex =>
          log.warn(s"Failed to extract Doris write metadata: ${ex.getMessage}")
          val fallbackDb = cmd.options.get("doris.table.identifier").map(_.split("\\.").headOption.getOrElse("unknown")).getOrElse("unknown")
          val fallbackTable = cmd.options.get("doris.table.identifier").map(_.split("\\.").lastOption.getOrElse("unknown")).getOrElse("unknown")
          WriteNodeInfo(DorisPlugin.asSourceId(fallbackDb, fallbackTable), cmd.mode, cmd.query, cmd.options)
      }.get
  }

  /**
   * 处理Doris V2读操作
   */
  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case plan if isDorisV2ReadPlan(plan) =>
      log.info(s" Detected Doris READ_V2 operation - Class: ${plan.getClass.getSimpleName}")
      // 对于V2操作，使用简化的处理方式
      ReadNodeInfo(DorisPlugin.asSourceId("unknown", "unknown"), Map("plan_type" -> plan.getClass.getSimpleName))
  }

  /**
   * 处理Doris V2写操作
   */
  override val writeNodeProcessor: PartialFunction[(SplineAgent.FuncName, LogicalPlan), WriteNodeInfo] = {
    case (_, plan) if isDorisV2WritePlan(plan) =>
      log.info(s" Detected Doris WRITE_V2 operation - Class: ${plan.getClass.getSimpleName}")
      
      // 尝试从LogicalPlan中提取数据库和表信息
      val (database, table, fenodes) = extractV2WriteMetadata(plan)
      val params = createTableIdentifier(database, table) ++ Map("plan_type" -> plan.getClass.getSimpleName)
      
      WriteNodeInfo(
        srcId = if (fenodes != "unknown") DorisPlugin.asSourceIdWithFenodes(fenodes, database, table) 
                else DorisPlugin.asSourceId(database, table),
        saveMode = SaveMode.Overwrite,
        logicalPlan = plan,
        params = params
      )
  }

  /**
   * 格式名称解析
   */
  override def formatNameResolver: PartialFunction[AnyRef, String] = {
    case "doris" => "doris"
    case className: String if className.toLowerCase.contains("doris") => "doris"
    case DorisSourceExtractor(_) => "doris"
  }

  // ========== 简化的辅助方法 ==========

  private def isDorisV2ReadPlan(plan: LogicalPlan): Boolean = {
    val className = plan.getClass.getSimpleName
    val planString = plan.toString
    className.contains("DataSourceV2Relation") && planString.toLowerCase.contains("doris")
  }

  private def isDorisV2WritePlan(plan: LogicalPlan): Boolean = {
    val className = plan.getClass.getSimpleName
    val planString = plan.toString
    className.contains("OverwriteByExpression") && planString.toLowerCase.contains("doris")
  }

  private def isDorisProvider(provider: AnyRef): Boolean = {
    provider match {
      case "doris" => true
      case s: String if s.toLowerCase.contains("doris") => true
      case DorisSourceExtractor(_) => true
      case _ => false
    }
  }

  private def extractConnectionParams(dorisRelation: AnyRef): Map[String, Any] = {
    Try {
      val params = scala.collection.mutable.Map[String, Any]()
      Try(extractValue[String](dorisRelation, "fenodes")).foreach(params += "fenodes" -> _)
      Try(extractValue[String](dorisRelation, "user")).foreach(params += "user" -> _)
      params.toMap
    }.getOrElse(Map.empty)
  }

  private def extractDatabaseFromOptions(options: Map[String, String]): String = {
    options.get("doris.table.identifier")
      .orElse(options.get("table.identifier"))
      .map(_.split("\\.").headOption.getOrElse("unknown"))
      .getOrElse("unknown")
  }

  private def extractTableFromOptions(options: Map[String, String]): String = {
    options.get("doris.table.identifier")
      .orElse(options.get("table.identifier"))
      .map(_.split("\\.").lastOption.getOrElse("unknown"))
      .getOrElse("unknown")
  }

  private def extractFenodesFromOptions(options: Map[String, String]): String = {
    options.get("doris.fenodes")
      .orElse(options.get("fenodes"))
      .getOrElse("unknown")
  }

  private def createTableIdentifier(database: String, table: String): Map[String, Any] = {
    Map(
      "table" -> Map(
        "identifier" -> Map(
          "database" -> database,
          "table" -> table
        )
      )
    )
  }

  /**
   * 从V2写操作的LogicalPlan中提取元数据
   */
  private def extractV2WriteMetadata(plan: LogicalPlan): (String, String, String) = {
    Try {
      val planString = plan.toString
      log.debug(s"Extracting metadata from V2 plan: $planString")
      
      // 尝试使用反射从plan中提取writeOptions
      val (database, table, fenodes) = Try {
        // 尝试多种可能的字段名
        val writeOptions = Try(extractValue[Map[String, String]](plan, "writeOptions"))
          .orElse(Try(extractValue[Map[String, String]](plan, "options")))
          .orElse(Try {
            // 尝试从子节点中提取
            val children = extractValue[Seq[AnyRef]](plan, "children")
            children.headOption.map(child => extractValue[Map[String, String]](child, "writeOptions")).getOrElse(Map.empty)
          })
          .getOrElse(Map.empty)
        
        log.info(s"Found writeOptions: $writeOptions")
        
        val tableIdentifier = writeOptions.getOrElse("doris.table.identifier", "unknown.unknown")
        val parts = tableIdentifier.split("\\.")
        val db = if (parts.length >= 2) parts(0) else "unknown"
        val tbl = if (parts.length >= 2) parts(1) else "unknown"
        val fn = writeOptions.getOrElse("doris.fenodes", "unknown")
        
        (db, tbl, fn)
      }.recover {
        case ex =>
          log.warn(s"Failed to extract from writeOptions, trying string parsing: ${ex.getMessage}")
          
          // 备用方案：从字符串中解析
          val database = extractFromPlanString(planString, "database")
          val table = extractFromPlanString(planString, "table")
          val fenodes = extractFromPlanString(planString, "fenodes")
          
          // 尝试从doris.table.identifier中提取
          val tableIdPattern = raw"""doris\.table\.identifier["']?\s*[=:]\s*["']?([^,\s"']+)""".r
          val fenodesPattern = raw"""doris\.fenodes["']?\s*[=:]\s*["']?([^,\s"']+)""".r
          
          val extractedTable = tableIdPattern.findFirstMatchIn(planString)
            .map(_.group(1))
            .getOrElse("unknown.unknown")
          
          val extractedFenodes = fenodesPattern.findFirstMatchIn(planString)
            .map(_.group(1))
            .getOrElse("unknown")
          
          val parts = extractedTable.split("\\.")
          val finalDb = if (parts.length >= 2) parts(0) else database
          val finalTable = if (parts.length >= 2) parts(1) else table
          
          (finalDb, finalTable, extractedFenodes)
      }.get
      
      log.info(s"Extracted V2 metadata - database: $database, table: $table, fenodes: $fenodes")
      (database, table, fenodes)
    }.recover {
      case ex =>
        log.warn(s"Failed to extract V2 metadata: ${ex.getMessage}")
        ("unknown", "unknown", "unknown")
    }.get
  }

  /**
   * 从plan字符串中提取特定字段的值
   */
  private def extractFromPlanString(planString: String, fieldName: String): String = {
    // 尝试多种模式来匹配字段值
    val patterns = List(
      raw"$fieldName[=:]\s*([^,\s\)]+)".r,
      raw"'$fieldName'[=:]\s*'([^']+)'".r,
      raw""""$fieldName"[=:]\s*"([^"]+)"""".r
    )
    
    patterns.flatMap(_.findFirstMatchIn(planString))
      .headOption
      .map(_.group(1))
      .getOrElse("unknown")
  }
}

object DorisPlugin {

  /**
   * 简化的Doris关系提取器
   */
  private object `_: DorisRelation` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.doris.spark.sql.sources.DorisRelation"
  )

  /**
   * 简化的Doris源提取器
   */
  private object DorisSourceExtractor extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.doris.spark.sql.sources.DefaultSource"
  )

  /**
   * 创建标准的源标识符
   */
  private def asSourceId(database: String, table: String): SourceIdentifier = {
    SourceIdentifier(Some("doris"), s"doris://$database/$table")
  }

  /**
   * 创建带fenodes的源标识符
   */
  private def asSourceIdWithFenodes(fenodes: String, database: String, table: String): SourceIdentifier = {
    SourceIdentifier(Some("doris"), s"doris://$fenodes/$database/$table")
  }
}
