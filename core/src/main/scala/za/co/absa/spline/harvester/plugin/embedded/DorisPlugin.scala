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
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SaveMode
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DorisPlugin._
import za.co.absa.spline.commons.reflect.extractors.AccessorMethodValueExtractor
import za.co.absa.spline.harvester.plugin.{Plugin, WriteNodeProcessing, ReadNodeProcessing}
import za.co.absa.spline.agent.SplineAgent
import javax.annotation.Priority
import scala.util.{Try}
import org.slf4j.LoggerFactory


@Priority(Precedence.Highest)
class DorisPlugin extends Plugin with WriteNodeProcessing  with ReadNodeProcessing {

  private val log = LoggerFactory.getLogger(classOf[DorisPlugin])

  override val readNodeProcessor: PartialFunction[LogicalPlan, ReadNodeInfo] = {
    // 处理所有DataSourceV2Relation，确保不调用父类的extractSourceIdFromTable方法
    case `_: DataSourceV2Relation`(relation) =>
      try {
        val table = extractValue[AnyRef](relation, "table")
        val tableName = Try(extractValue[String](table, "name")).getOrElse("unknown")
        val identifier = extractValue[AnyRef](relation, "identifier")
        val options = extractValue[AnyRef](relation, "options")
        val tableProps = Try(extractValue[java.util.Map[String, String]](table, "properties")).getOrElse(new java.util.HashMap[String, String]())
        val provider = Option(tableProps.get("provider")).getOrElse("")
        val isDorisTable = table.getClass.getName.contains("Doris") || provider.toLowerCase.contains("doris")

        val sourceId = if (isDorisTable) {
          log.info("检测到Doris表，使用特殊处理逻辑")
          SourceIdentifier(Some("doris"), s"doris:$tableName")
        } else {
          // 对于非Doris表，直接返回默认SourceIdentifier
          log.warn(s"检测到非Doris表: ${table.getClass.getName}，使用默认SourceIdentifier")
          SourceIdentifier(Some("unknown"), "unknown:unknown")
        }

        val props = Map(
          "table" -> Map("identifier" -> tableName),
          "identifier" -> identifier,
          "options" -> options)
        ReadNodeInfo(sourceId, props)
      } catch {
        case ex: Exception =>
          log.error(s"处理DataSourceV2Relation时发生错误: ${ex.getMessage}", ex)
          ReadNodeInfo(SourceIdentifier(Some("unknown"), "unknown:unknown"), Map.empty)
      }



    case plan if isDorisV2ReadPlan(plan) =>{
      log.info(s"检测到Doris READ_V2操作 - 类名: ${plan.getClass.getSimpleName}")

      // 尝试从plan中提取必要信息
      val (database, table) = extractTableIdentifierFromV2ReadPlan(plan)
      log.info(s"提取到表标识符 - 数据库: $database, 表: $table")

      // 尝试提取options和identifier
      val options = Try(extractValue[Map[String, String]](plan, "options"))
        .orElse(Try(extractValue[Map[String, String]](plan, "readOptions")))
        .getOrElse(Map.empty)

      val identifier = Try(extractValue[AnyRef](plan, "identifier")).getOrElse(null)

      val props = Map(
        "table" -> Map("identifier" -> s"$database.$table"),
        "identifier" -> identifier,
        "options" -> options
      ) ++ createTableIdentifier(database, table) + ("plan_type" -> plan.getClass.getSimpleName)

      // 创建SourceIdentifier并返回ReadNodeInfo
      val sourceId = asSourceId(database, table)
      log.info(s"创建的SourceIdentifier: $sourceId")
      ReadNodeInfo(sourceId, props)
    }

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


  private def isDorisV2ReadPlan(plan: LogicalPlan): Boolean = {
    val className = plan.getClass.getSimpleName
    val planString = plan.toString.toLowerCase
    // 检查是否是DataSourceV2Relation类型
    val isDataSourceV2Relation = className.contains("DataSourceV2Relation")
    // 检查计划字符串中是否包含doris相关标识
    val containsDoris = planString.contains("doris")
    // 检查是否包含LogicalRelation
    val containsLogicalRelation = planString.contains("logicalrelation")
    // 检查是否包含DorisRelation
    val containsDorisRelation = planString.contains("dorisrelation")
    // 检查是否包含org.apache.doris.spark.sql
    val containsDorisPackage = planString.contains("org.apache.doris.spark.sql")
    // 满足以下任一条件即为Doris V2读取操作
    (isDataSourceV2Relation && containsDoris) || containsDorisRelation || (containsLogicalRelation && containsDorisPackage) || (className.contains("Relation") && containsDorisPackage)
  }


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

  private def extractTableIdentifierFromV2ReadPlan(plan: LogicalPlan): (String, String) = {
    log.info("开始从V2读操作中提取表标识符")
    if (plan.getClass.getSimpleName.contains("LogicalRelation")) {
      val relation = extractValue[AnyRef](plan, "relation")
      log.info(s"提取到relation: $relation")
      val isDorisRelation = relation.getClass.getName.contains("DorisRelation")
      if (isDorisRelation) {
        val parameters = Try(extractValue[Map[String, String]](relation, "parameters")).getOrElse(Map.empty)
        log.info(s"从DorisRelation提取到parameters: $parameters")

        val tableIdentifierFromParams = parameters.keys.find(key =>
            key.toLowerCase.contains("doris") &&
              (key.toLowerCase.contains("table") || key.toLowerCase.contains("identifier")))
          .flatMap(parameters.get)
          .orElse(parameters.get("table.identifier"))
          .orElse(parameters.get("dbtable"))
          .orElse(parameters.get("table"))
          .getOrElse("unknown.unknown")

        // 如果从parameters中成功提取到表标识符，则直接使用
        if (tableIdentifierFromParams != "unknown.unknown") {
          log.info("使用从parameters中提取的表标识符")
          val tableIdentifierStr = tableIdentifierFromParams.toString
          val parts = tableIdentifierStr.split("\\.")
          val database = if (parts.length >= 2) parts(0) else "unknown"
          val table = if (parts.length >= 2) parts(1) else "unknown"
          log.info(s"解析结果 - 数据库: $database, 表: $table")
          return (database, table)
        }

      } else {
        log.warn("提取到的relation不是DorisRelation类型")
      }
    } else {
      log.warn("plan不是LogicalRelation类型")
    }
    log.warn("所有提取方法都失败，返回默认值")
    ("unknown", "unknown")

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

  object DorisSourceExtractor extends SafeTypeMatchingExtractor[AnyRef]("org.apache.doris.spark.sql.DorisSourceProvider")


  object `_: DataSourceV2Relation` extends SafeTypeMatchingExtractor[AnyRef]( "org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation")



  def asSourceId(database: String, table: String): SourceIdentifier = {
    val safeDatabase = Option(database).getOrElse("unknown")
    val safeTable = Option(table).getOrElse("unknown")
    val sourceId = s"doris:$safeDatabase.$safeTable"
    SourceIdentifier(Some("doris"), sourceId)
  }


   def asSourceIdWithFenodes(fenodes: String, database: String, table: String): SourceIdentifier = {
    val sourceId = s"doris://$fenodes/$database/$table"
    SourceIdentifier(Some("doris"), sourceId)
  }
}
