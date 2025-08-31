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
import za.co.absa.spline.commons.reflect.extractors.AccessorMethodValueExtractor
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
      log.info("开始处理传统的Doris读操作")
      Try {
        log.debug("尝试从DorisRelation中提取数据库和表信息")
        val database = extractValue[String](dorisRelation, "database")
        val table = extractValue[String](dorisRelation, "table")
        log.info(s"成功提取到数据库: $database, 表: $table")
        
        log.debug("提取连接参数")
        val params = extractConnectionParams(dorisRelation) ++ createTableIdentifier(database, table)
        log.debug(s"连接参数: $params")
        
        log.info("成功创建Doris读操作的ReadNodeInfo")
        ReadNodeInfo(DorisPlugin.asSourceId(database, table), params)
      }.recover {
        case ex =>
          log.error(s"提取Doris读操作元数据失败: ${ex.getMessage}", ex)
          log.warn("使用默认值创建ReadNodeInfo")
          ReadNodeInfo(DorisPlugin.asSourceId("unknown", "unknown"), Map("error" -> ex.getMessage))
      }.get
  }

  /**
   * 处理传统的Doris写操作
   */
  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if isDorisProvider(rp) =>
      log.info("开始处理传统的Doris写操作")
      log.debug(s"检测到Doris提供者: $rp")
      Try {
        log.debug("从命令选项中提取数据库和表信息")
        val database = extractDatabaseFromOptions(cmd.options)
        val table = extractTableFromOptions(cmd.options)
        val fenodes = extractFenodesFromOptions(cmd.options)
        log.info(s"提取到数据库: $database, 表: $table, fenodes: $fenodes")
        
        log.debug("创建增强选项")
        val enhancedOptions = cmd.options ++ createTableIdentifier(database, table)
        log.debug(s"增强选项: $enhancedOptions")
        
        log.info("成功创建Doris写操作的WriteNodeInfo")
        WriteNodeInfo(DorisPlugin.asSourceIdWithFenodes(fenodes, database, table), cmd.mode, cmd.query, enhancedOptions)
      }.recover {
        case ex =>
          log.error(s"提取Doris写操作元数据失败: ${ex.getMessage}", ex)
          log.warn("使用备选方案创建WriteNodeInfo")
          val fallbackDb = cmd.options.get("doris.table.identifier").map(_.split("\\.").headOption.getOrElse("unknown")).getOrElse("unknown")
          val fallbackTable = cmd.options.get("doris.table.identifier").map(_.split("\\.").lastOption.getOrElse("unknown")).getOrElse("unknown")
          log.info(s"使用备选值 - 数据库: $fallbackDb, 表: $fallbackTable")
          WriteNodeInfo(DorisPlugin.asSourceId(fallbackDb, fallbackTable), cmd.mode, cmd.query, cmd.options)
      }.get
  }
  
  /**
   * 处理Doris写操作，确保能捕获所有Doris相关的SaveIntoDataSourceCommand
   */
  override def writeNodeProcessor: PartialFunction[(SplineAgent.FuncName, LogicalPlan), WriteNodeInfo] = {
    case (_, cmd: SaveIntoDataSourceCommand) if isDorisSaveCommand(cmd) =>
      log.info("通过writeNodeProcessor处理Doris SaveIntoDataSourceCommand")
      log.debug(s"命令选项: ${cmd.options}")
      
      Try {
        log.debug("从命令选项中提取数据库和表信息")
        val database = extractDatabaseFromOptions(cmd.options)
        val table = extractTableFromOptions(cmd.options)
        val fenodes = extractFenodesFromOptions(cmd.options)
        log.info(s"提取到数据库: $database, 表: $table, fenodes: $fenodes")
        
        log.debug("创建增强选项")
        val enhancedOptions = cmd.options ++ createTableIdentifier(database, table)
        log.debug(s"增强选项: $enhancedOptions")
        
        log.info("成功创建Doris写操作的WriteNodeInfo")
        WriteNodeInfo(DorisPlugin.asSourceIdWithFenodes(fenodes, database, table), cmd.mode, cmd.query, enhancedOptions)
      }.recover {
        case ex =>
          log.error(s"提取Doris写操作元数据失败: ${ex.getMessage}", ex)
          log.warn("使用备选方案创建WriteNodeInfo")
          val fallbackDb = cmd.options.get("doris.table.identifier").map(_.split("\\.").headOption.getOrElse("unknown")).getOrElse("unknown")
          val fallbackTable = cmd.options.get("doris.table.identifier").map(_.split("\\.").lastOption.getOrElse("unknown")).getOrElse("unknown")
          log.info(s"使用备选值 - 数据库: $fallbackDb, 表: $fallbackTable")
          WriteNodeInfo(DorisPlugin.asSourceId(fallbackDb, fallbackTable), cmd.mode, cmd.query, cmd.options)
      }.get
      
    case (_, plan) if isDorisV2WritePlan(plan) =>
      log.info(s"检测到Doris WRITE_V2操作 - 类名: ${plan.getClass.getSimpleName}")
      log.debug(s"计划详情: $plan")
      
      // 尝试从LogicalPlan中提取数据库和表信息
      log.debug("开始从V2计划中提取元数据")
      val (database, table, fenodes) = extractV2WriteMetadata(plan)
      log.info(s"提取到元数据 - 数据库: $database, 表: $table, fenodes: $fenodes")
      
      log.debug("创建参数映射")
      val params = createTableIdentifier(database, table) ++ Map("plan_type" -> plan.getClass.getSimpleName)
      log.debug(s"参数映射: $params")
      
      log.info("成功创建Doris V2写操作的WriteNodeInfo")
      WriteNodeInfo(
        srcId = if (fenodes != "unknown") DorisPlugin.asSourceIdWithFenodes(fenodes, database, table) 
                else DorisPlugin.asSourceId(database, table),
        saveMode = SaveMode.Overwrite,
        logicalPlan = plan,
        params = params
      )
  }

  /**
   * 处理Doris V2读操作
   */
  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case plan if isDorisV2ReadPlan(plan) =>
      log.info(s"检测到Doris READ_V2操作 - 类名: ${plan.getClass.getSimpleName}")
      log.debug(s"计划详情: $plan")
      // 对于V2操作，使用简化的处理方式
      log.info("使用简化方式处理V2读操作")
      ReadNodeInfo(DorisPlugin.asSourceId("unknown", "unknown"), Map("plan_type" -> plan.getClass.getSimpleName))
  }



  /**
   * 格式名称解析
   */
  override def formatNameResolver: PartialFunction[AnyRef, String] = {
    case "doris" => "doris"
    case className: String if className.toLowerCase.contains("doris") => "doris"
    case className: String if className.toLowerCase.contains("dorisconnector") => "doris"
    case className: String if className.toLowerCase.contains("doris.source") => "doris"
    case className: String if className.toLowerCase.contains("doris.provider") => "doris"
    case DorisSourceExtractor(_) => "doris"
    case provider if isDorisProvider(provider) => "doris"
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
      case className: String => 
        // 检查类名是否包含各种可能的Doris连接器类名模式
        val lowerClassName = className.toLowerCase
        lowerClassName.contains("doris") && 
        (lowerClassName.contains("source") || 
         lowerClassName.contains("provider") || 
         lowerClassName.contains("connector"))
      case _ => false
    }
  }
  
  /**
   * 检测SaveIntoDataSourceCommand是否与Doris相关
   */
  // 创建一个与SaveIntoDataSourceCommandPlugin中相同的提取器
  private object RelationProviderExtractor extends AccessorMethodValueExtractor[AnyRef]("provider", "dataSource")
  
  private def isDorisSaveCommand(cmd: SaveIntoDataSourceCommand): Boolean = {
    val options = cmd.options
    // 检查选项中是否包含Doris特有的参数（支持多种命名方式）
    val hasDorisTableId = options.keys.exists(key => 
      key.toLowerCase.contains("doris") && 
      (key.toLowerCase.contains("table") || key.toLowerCase.contains("identifier")))
      
    val hasDorisFenodes = options.keys.exists(key => 
      key.toLowerCase.contains("doris") && 
      (key.toLowerCase.contains("fenodes") || key.toLowerCase.contains("fe") || key.toLowerCase.contains("host")))
      
    val hasDorisWriteMode = options.keys.exists(key => 
      key.toLowerCase.contains("doris") && 
      key.toLowerCase.contains("write"))
      
    val hasDorisConn = options.keys.exists(key => 
      key.toLowerCase.contains("doris") && 
      (key.toLowerCase.contains("conn") || key.toLowerCase.contains("url") || key.toLowerCase.contains("jdbc")))
      
    val hasDorisUser = options.keys.exists(key => 
      key.toLowerCase.contains("doris") && 
      key.toLowerCase.contains("user"))
      
    val hasDorisPassword = options.keys.exists(key => 
      key.toLowerCase.contains("doris") && 
      key.toLowerCase.contains("password"))
      
    val hasPathWithDoris = options.get("path").exists(_.toLowerCase.contains("doris"))
    
    // 检查provider是否为Doris
    val isDorisProviderMatch = RelationProviderExtractor.unapply(cmd).exists(isDorisProvider)
    
    // 检查format是否为doris
    val isDorisFormat = options.get("format").exists(_.toLowerCase.contains("doris"))
    
    // 满足任一条件即认为是Doris相关操作
    hasDorisTableId || hasDorisFenodes || hasDorisWriteMode || hasDorisConn || 
    hasDorisUser || hasDorisPassword || hasPathWithDoris || isDorisProviderMatch || isDorisFormat
  }

  private def extractConnectionParams(dorisRelation: AnyRef): Map[String, Any] = {
    log.debug("开始提取连接参数")
    val result = Try {
      val params = scala.collection.mutable.Map[String, Any]()
      Try(extractValue[String](dorisRelation, "fenodes")).foreach { fenodes =>
        log.debug(s"提取到fenodes: $fenodes")
        params += "fenodes" -> fenodes
      }
      Try(extractValue[String](dorisRelation, "user")).foreach { user =>
        log.debug(s"提取到user: $user")
        params += "user" -> user
      }
      params.toMap
    }.getOrElse(Map.empty)
    log.debug(s"连接参数提取结果: $result")
    result
  }

  private def extractDatabaseFromOptions(options: Map[String, String]): String = {
    log.debug(s"从选项中提取数据库名: $options")
    
    // 尝试多种可能的表标识符选项名称
    val tableIdentifier = options.keys.find(key => 
      key.toLowerCase.contains("doris") && 
      (key.toLowerCase.contains("table") || key.toLowerCase.contains("identifier")))
      .flatMap(options.get)
      .orElse(options.get("table.identifier"))
      .orElse(options.get("dbtable"))
      .orElse(options.get("table"))
      
    val result = tableIdentifier
      .map(_.split("\\.").headOption.getOrElse("unknown"))
      .getOrElse("unknown")
      
    log.debug(s"提取到的数据库名: $result")
    result
  }

  private def extractTableFromOptions(options: Map[String, String]): String = {
    log.debug(s"从选项中提取表名: $options")
    
    // 尝试多种可能的表标识符选项名称
    val tableIdentifier = options.keys.find(key => 
      key.toLowerCase.contains("doris") && 
      (key.toLowerCase.contains("table") || key.toLowerCase.contains("identifier")))
      .flatMap(options.get)
      .orElse(options.get("table.identifier"))
      .orElse(options.get("dbtable"))
      .orElse(options.get("table"))
      
    val result = tableIdentifier
      .map(_.split("\\.").lastOption.getOrElse("unknown"))
      .getOrElse("unknown")
      
    log.debug(s"提取到的表名: $result")
    result
  }

  private def extractFenodesFromOptions(options: Map[String, String]): String = {
    log.debug(s"从选项中提取fenodes: $options")
    
    // 尝试多种可能的fenodes选项名称
    val result = options.keys.find(key => 
      key.toLowerCase.contains("doris") && 
      (key.toLowerCase.contains("fenodes") || key.toLowerCase.contains("fe") || key.toLowerCase.contains("host")))
      .flatMap(options.get)
      .orElse(options.get("doris.fenodes"))
      .orElse(options.get("fenodes"))
      .orElse(options.get("fe"))
      .orElse(options.get("host"))
      .orElse(options.get("url"))
      .orElse(options.get("jdbcurl"))
      .getOrElse("unknown")
      
    log.debug(s"提取到的fenodes: $result")
    result
  }

  private def createTableIdentifier(database: String, table: String): Map[String, Any] = {
    log.debug(s"创建表标识符: 数据库=$database, 表=$table")
    val result = Map(
      "table" -> Map(
        "identifier" -> Map(
          "database" -> database,
          "table" -> table
        )
      )
    )
    log.debug(s"表标识符创建结果: $result")
    result
  }

  /**
   * 从V2写操作的LogicalPlan中提取元数据
   */
  private def extractV2WriteMetadata(plan: LogicalPlan): (String, String, String) = {
    log.debug("开始从V2写操作中提取元数据")
    Try {
      val planString = plan.toString
      log.debug(s"从V2计划中提取元数据: $planString")
      
      // 尝试使用反射从plan中提取writeOptions
      val (database, table, fenodes) = Try {
        log.debug("尝试使用反射提取writeOptions")
        // 尝试多种可能的字段名
        val writeOptions = Try(extractValue[Map[String, String]](plan, "writeOptions"))
          .orElse(Try(extractValue[Map[String, String]](plan, "options")))
          .orElse(Try {
            log.debug("尝试从子节点中提取writeOptions")
            // 尝试从子节点中提取
            val children = extractValue[Seq[AnyRef]](plan, "children")
            children.headOption.map(child => extractValue[Map[String, String]](child, "writeOptions")).getOrElse(Map.empty)
          })
          .getOrElse(Map.empty)
        
        log.info(s"找到writeOptions: $writeOptions")
        
        val tableIdentifier = writeOptions.getOrElse("doris.table.identifier", "unknown.unknown")
        log.debug(s"表标识符: $tableIdentifier")
        val parts = tableIdentifier.split("\\.")
        val db = if (parts.length >= 2) parts(0) else "unknown"
        val tbl = if (parts.length >= 2) parts(1) else "unknown"
        val fn = writeOptions.getOrElse("doris.fenodes", "unknown")
        
        log.debug(s"解析结果 - 数据库: $db, 表: $tbl, fenodes: $fn")
        (db, tbl, fn)
      }.recover {
        case ex =>
          log.warn(s"从writeOptions提取失败，尝试字符串解析: ${ex.getMessage}")
          
          // 备用方案：从字符串中解析
          log.debug("使用字符串解析方案")
          val database = extractFromPlanString(planString, "database")
          val table = extractFromPlanString(planString, "table")
          val fenodes = extractFromPlanString(planString, "fenodes")
          
          log.debug(s"字符串解析结果 - 数据库: $database, 表: $table, fenodes: $fenodes")
          
          // 尝试从doris.table.identifier中提取
          log.debug("尝试使用正则表达式提取")
          val tableIdPattern = raw"""doris\.table\.identifier["']?\s*[=:]\s*["']?([^,\s"']+)""".r
          val fenodesPattern = raw"""doris\.fenodes["']?\s*[=:]\s*["']?([^,\s"']+)""".r
          
          val extractedTable = tableIdPattern.findFirstMatchIn(planString)
            .map(_.group(1))
            .getOrElse("unknown.unknown")
          
          val extractedFenodes = fenodesPattern.findFirstMatchIn(planString)
            .map(_.group(1))
            .getOrElse("unknown")
          
          log.debug(s"正则表达式提取结果 - 表: $extractedTable, fenodes: $extractedFenodes")
          
          val parts = extractedTable.split("\\.")
          val finalDb = if (parts.length >= 2) parts(0) else database
          val finalTable = if (parts.length >= 2) parts(1) else table
          
          log.debug(s"最终结果 - 数据库: $finalDb, 表: $finalTable, fenodes: $extractedFenodes")
          (finalDb, finalTable, extractedFenodes)
      }.get
      
      log.info(s"成功提取V2元数据 - 数据库: $database, 表: $table, fenodes: $fenodes")
      (database, table, fenodes)
    }.recover {
      case ex =>
        log.error(s"提取V2元数据失败: ${ex.getMessage}", ex)
        ("unknown", "unknown", "unknown")
    }.get
  }

  /**
   * 从plan字符串中提取特定字段的值
   */
  private def extractFromPlanString(planString: String, fieldName: String): String = {
    log.debug(s"从计划字符串中提取字段: $fieldName")
    // 尝试多种模式来匹配字段值
    val patterns = List(
      raw"$fieldName[=:]\s*([^,\s\)]+)".r,
      raw"'$fieldName'[=:]\s*'([^']+)'".r,
      raw""""$fieldName"[=:]\s*"([^"]+)"""".r
    )
    
    val result = patterns.flatMap(_.findFirstMatchIn(planString))
      .headOption
      .map(_.group(1))
      .getOrElse("unknown")
    
    log.debug(s"字段提取结果 - $fieldName: $result")
    result
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
    "org.apache.doris.spark.sql.DorisSourceProvider"
  )

  /**
   * 创建标准的源标识符
   */
  private def asSourceId(database: String, table: String): SourceIdentifier = {
    val sourceId = s"doris://$database/$table"
    println(s"创建标准源标识符: $sourceId") // 使用println因为object中无法访问log
    SourceIdentifier(Some("doris"), sourceId)
  }

  /**
   * 创建带fenodes的源标识符
   */
  private def asSourceIdWithFenodes(fenodes: String, database: String, table: String): SourceIdentifier = {
    val sourceId = s"doris://$fenodes/$database/$table"
    println(s"创建带fenodes的源标识符: $sourceId") // 使用println因为object中无法访问log
    SourceIdentifier(Some("doris"), sourceId)
  }
}
