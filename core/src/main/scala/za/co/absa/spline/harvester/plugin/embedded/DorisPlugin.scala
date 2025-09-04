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
import org.slf4j.LoggerFactory


@Priority(Precedence.Highest)
class DorisPlugin
  extends Plugin
    with BaseRelationProcessing
    with WriteNodeProcessing
    with ReadNodeProcessing {


  private val log = LoggerFactory.getLogger(classOf[DorisPlugin])

  /**
   * 处理传统的Doris读操作
   */
  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: DorisRelation`(dorisRelation), _) =>
      log.info("开始处理传统的Doris读操作",dorisRelation.toString)
      Try {
        val database = extractValue[String](dorisRelation, "database")
        val table = extractValue[String](dorisRelation, "table")
        log.info(s"成功提取到数据库: $database, 表: $table")

        log.info("提取连接参数")
        val params = extractConnectionParams(dorisRelation) ++ createTableIdentifier(database, table)
        log.info(s"连接参数: $params")

        log.info("成功创建Doris读操作的ReadNodeInfo")
        log.info("这个是ReadNodeInfo", ReadNodeInfo(DorisPlugin.asSourceId(database, table), params))
        ReadNodeInfo(DorisPlugin.asSourceId(database, table), params)
      }.recover {
        case ex =>
          log.error(s"提取Doris读操作元数据失败: ${ex.getMessage}", ex)
          log.warn("使用默认值创建ReadNodeInfo")
          ReadNodeInfo(DorisPlugin.asSourceId("unknown", "unknown"), Map("error" -> ex.getMessage))
      }.get
  }


  /**
   * 处理Doris写操作，确保能捕获所有Doris相关的SaveIntoDataSourceCommand
   */
  override def writeNodeProcessor: PartialFunction[(SplineAgent.FuncName, LogicalPlan), WriteNodeInfo] = {
    case (_, cmd: SaveIntoDataSourceCommand) if isDorisSaveCommand(cmd) =>
        val database = extractDatabaseFromOptions(cmd.options)
        val table = extractTableFromOptions(cmd.options)
        val fenodes = extractFenodesFromOptions(cmd.options)
        val enhancedOptions = cmd.options ++ createTableIdentifier(database, table)
        WriteNodeInfo(DorisPlugin.asSourceIdWithFenodes(fenodes, database, table), cmd.mode, cmd.query, enhancedOptions)

      case (_, plan) if isDorisV2WritePlan(plan) =>
        log.info(s"检测到Doris WRITE_V2操作 - 类名: ${plan.getClass.getSimpleName}")
        val (database, table, fenodes) = extractV2WriteMetadata(plan)
        log.info(s"提取到元数据 - 数据库: $database, 表: $table, fenodes: $fenodes")
        val params = createTableIdentifier(database, table) ++ Map("plan_type" -> plan.getClass.getSimpleName)
        val originalPlan = extractOriginalPlan(plan)

        
        WriteNodeInfo(
          srcId = if (fenodes != "unknown") DorisPlugin.asSourceIdWithFenodes(fenodes, database, table) else DorisPlugin.asSourceId(database, table),
          saveMode = SaveMode.Overwrite,
          logicalPlan = originalPlan,
          params = params
        )
}

  /**
   * 处理Doris V2读操作
   */
  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    case plan if isDorisV2ReadPlan(plan) =>
      log.info(s"检测到Doris READ_V2操作 - 类名: ${plan.getClass.getSimpleName}")
      log.info(s"计划详情: $plan")
      // 对于V2操作，使用简化的处理方式
      log.info("使用简化方式处理V2读操作")
      ReadNodeInfo(DorisPlugin.asSourceId("unknown", "unknown"), Map("plan_type" -> plan.getClass.getSimpleName))
  }


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
        val lowerClassName = className.toLowerCase
        lowerClassName.contains("doris") && (lowerClassName.contains("source") || lowerClassName.contains("provider") || lowerClassName.contains("connector"))
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

    log.info(s"开始打印options.keys: ${options.keys}")
    val hasDorisTableId = options.keys.exists(key => key.toLowerCase.contains("doris") &&  (key.toLowerCase.contains("table") || key.toLowerCase.contains("identifier")))

    val hasDorisFenodes = options.keys.exists(key => key.toLowerCase.contains("doris") && (key.toLowerCase.contains("fenodes") || key.toLowerCase.contains("fe") || key.toLowerCase.contains("host")))

    val hasDorisWriteMode = options.keys.exists(key => key.toLowerCase.contains("doris") && key.toLowerCase.contains("write"))

    val hasDorisConn = options.keys.exists(key => key.toLowerCase.contains("doris") && (key.toLowerCase.contains("conn") || key.toLowerCase.contains("url") || key.toLowerCase.contains("jdbc")))

    val hasDorisUser = options.keys.exists(key => key.toLowerCase.contains("doris") && key.toLowerCase.contains("user"))

    val hasDorisPassword = options.keys.exists(key => key.toLowerCase.contains("doris") && key.toLowerCase.contains("password"))

    val hasPathWithDoris = options.get("path").exists(_.toLowerCase.contains("doris"))
    log.info(s"开始打印options.path: ${options.get("path")}")

    val isDorisProviderMatch = RelationProviderExtractor.unapply(cmd).exists(isDorisProvider)

    log.info(s"开始打印isDorisProviderMatch: $isDorisProviderMatch")

    val isDorisFormat = options.get("format").exists(_.toLowerCase.contains("doris"))

    hasDorisTableId || hasDorisFenodes || hasDorisWriteMode || hasDorisConn || hasDorisUser || hasDorisPassword || hasPathWithDoris || isDorisProviderMatch || isDorisFormat
  }

  private def extractConnectionParams(dorisRelation: AnyRef): Map[String, Any] = {
    log.info("开始提取连接参数")
    val result = Try {
      val params = scala.collection.mutable.Map[String, Any]()
      Try(extractValue[String](dorisRelation, "fenodes")).foreach { fenodes =>
        log.info(s"提取到fenodes: $fenodes")
        params += "fenodes" -> fenodes
      }
      Try(extractValue[String](dorisRelation, "user")).foreach { user =>
        log.info(s"提取到user: $user")
        params += "user" -> user
      }
      params.toMap
    }.getOrElse(Map.empty)
    log.info(s"连接参数提取结果: $result")
    result
  }

  private def extractDatabaseFromOptions(options: Map[String, String]): String = {
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
    result
  }

  private def extractTableFromOptions(options: Map[String, String]): String = {

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
    result
  }

  private def extractFenodesFromOptions(options: Map[String, String]): String = {

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
    result
  }

  private def createTableIdentifier(database: String, table: String): Map[String, Any] = {
    val result = Map(
      "table" -> Map(
        "identifier" -> Map(
          "database" -> database,
          "table" -> table
        )
      )
    )
    result
  }

  /**
   * 从V2写操作的LogicalPlan中提取元数据
   */
  private def extractV2WriteMetadata(plan: LogicalPlan): (String, String, String) = {
    log.info("开始从V2写操作中提取元数据")
    Try {
      val planString = plan.toString
        val writeOptions = Try(extractValue[Map[String, String]](plan, "writeOptions"))
          .orElse(Try(extractValue[Map[String, String]](plan, "options")))
          .orElse(Try {
            log.info("尝试从子节点中提取writeOptions")
            // 尝试从子节点中提取
            val children = extractValue[Seq[AnyRef]](plan, "children")
            children.headOption.map(child => extractValue[Map[String, String]](child, "writeOptions")).getOrElse(Map.empty)
          })
          .getOrElse(Map.empty)

        log.info(s"找到writeOptions: $writeOptions")

        val tableIdentifier = writeOptions.getOrElse("doris.table.identifier", "unknown.unknown")
        log.info(s"表标识符: $tableIdentifier")
        val parts = tableIdentifier.split("\\.")
        val db = if (parts.length >= 2) parts(0) else "unknown"
        val tbl = if (parts.length >= 2) parts(1) else "unknown"
        val fn = writeOptions.getOrElse("doris.fenodes", "unknown")
        log.info(s"解析结果 - 数据库: $db, 表: $tbl, fenodes: $fn")
        (db, tbl, fn)
    }.recover {
      case ex =>
        log.error(s"提取V2元数据失败: ${ex.getMessage}", ex)
        ("unknown", "unknown", "unknown")
    }.get
  }
  
  /**
   * 从V2写操作中提取原始查询计划，去除多余的Project操作和OverwriteByExpression操作
   */
  private def extractOriginalPlan(plan: LogicalPlan): LogicalPlan = {
    log.info("开始提取原始查询计划")
    Try {
      var currentPlan = plan

      if (currentPlan.getClass.getSimpleName.contains("OverwriteByExpression")) {
        val children = extractValue[Seq[AnyRef]](currentPlan, "children")
        if (children.nonEmpty) {
          currentPlan = children.head.asInstanceOf[LogicalPlan]
        }
      }
      log.info(s"最终提取到的原始查询计划: ${currentPlan.getClass.getSimpleName}")
      currentPlan
    }.getOrElse(plan)
  }

}

object DorisPlugin {

  private object `_: DorisRelation` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.doris.spark.sql.sources.DorisRelation"
  )

  private object DorisSourceExtractor extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.doris.spark.sql.DorisSourceProvider"
  )

  private def asSourceId(database: String, table: String): SourceIdentifier = {
    val sourceId = s"doris://$database/$table"
    println(s"创建标准源标识符: $sourceId") // 使用println因为object中无法访问log
    SourceIdentifier(Some("doris"), sourceId)
  }

  private def asSourceIdWithFenodes(fenodes: String, database: String, table: String): SourceIdentifier = {
    val sourceId = s"doris://$fenodes/$database/$table"
    println(s"创建带fenodes的源标识符: $sourceId") // 使用println因为object中无法访问log
    SourceIdentifier(Some("doris"), sourceId)
  }
}
