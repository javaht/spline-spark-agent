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

import org.apache.spark.sql.execution.datasources.{LogicalRelation, SaveIntoDataSourceCommand}
import org.apache.spark.sql.sources.BaseRelation
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DorisPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, Plugin, RelationProviderProcessing}

import scala.util.control.NonFatal

import javax.annotation.Priority

/**
 * Plugin for capturing lineage information from Apache Doris data source operations.
 * 
 * This plugin handles both read and write operations through Spark's Doris connector,
 * extracting metadata such as database names, table names, connection parameters,
 * and write-specific options to build comprehensive lineage information.
 * 
 * The plugin supports:
 * - Read operations from Doris tables via LogicalRelation processing
 * - Write operations to Doris tables via SaveIntoDataSourceCommand processing
 * - Graceful handling of missing Doris connector classes
 * - Standardized source identifier generation in format "doris://database/table"
 * - Parameter extraction and normalization for connection details
 * 
 * @see [[za.co.absa.spline.harvester.plugin.BaseRelationProcessing]]
 * @see [[za.co.absa.spline.harvester.plugin.RelationProviderProcessing]]
 */
@Priority(Precedence.Normal)
class DorisPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing {

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: DorisRelation`(dorisRelation), _) =>
      try {
        // Extract database and table information from Doris relation
        val database = extractValue[String](dorisRelation, "database")
        val table = extractValue[String](dorisRelation, "table")
        
        // Extract connection parameters
        val params = extractDorisReadParams(dorisRelation)
        
        ReadNodeInfo(asSourceId(database, table), params)
      } catch {
        case _: ClassNotFoundException =>
          // Doris connector classes not available
          ReadNodeInfo(asSourceId("unknown", "unknown"), Map("error" -> "doris_connector_unavailable"))
        case _: ReflectiveOperationException =>
          // Reflection failed, possibly due to version mismatch
          ReadNodeInfo(asSourceId("unknown", "unknown"), Map("error" -> "reflection_failed"))
        case NonFatal(_) =>
          // General fallback for other extraction failures
          ReadNodeInfo(asSourceId("unknown", "unknown"), Map.empty)
      }
  }

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if isDorisProvider(rp) =>
      try {
        // Extract database and table from command options
        val database = cmd.options.getOrElse("database", "default")
        val table = cmd.options.getOrElse("table", cmd.options.getOrElse("doris.table.identifier", "unknown"))
        
        // Extract write-specific parameters
        val params = extractDorisWriteParams(cmd.options)
        
        WriteNodeInfo(
          srcId = asSourceId(database, table),
          saveMode = cmd.mode,
          logicalPlan = cmd.query,
          params = params
        )
      } catch {
        case _: ClassNotFoundException =>
          // Doris connector classes not available
          WriteNodeInfo(
            srcId = asSourceId("unknown", "unknown"),
            saveMode = cmd.mode,
            logicalPlan = cmd.query,
            params = cmd.options + ("error" -> "doris_connector_unavailable")
          )
        case NonFatal(_) =>
          // General fallback for other extraction failures
          WriteNodeInfo(
            srcId = asSourceId("unknown", "unknown"),
            saveMode = cmd.mode,
            logicalPlan = cmd.query,
            params = cmd.options
          )
      }
  }
}

object DorisPlugin {

  /**
   * Type extractor for Doris BaseRelation objects.
   * Safely matches org.apache.doris.spark.sql.DorisRelation instances.
   */
  private object `_: DorisRelation` extends SafeTypeMatchingExtractor[AnyRef]("org.apache.doris.spark.sql.DorisRelation")

  /**
   * Type extractor for Doris data source provider.
   * Matches the DefaultSource class from Doris Spark connector.
   */
  private object DorisDataSourceExtractor extends SafeTypeMatchingExtractor[AnyRef]("org.apache.doris.spark.sql.DefaultSource")

  /**
   * Extracts connection parameters from Doris relation for read operations.
   * 
   * @param dorisRelation the Doris relation object
   * @return Map of connection parameters
   */
  private def extractDorisReadParams(dorisRelation: AnyRef): Map[String, Any] = {
    try {
      val params = scala.collection.mutable.Map[String, Any]()
      
      // Try to extract common Doris connection parameters
      try {
        val fenodes = extractValue[String](dorisRelation, "fenodes")
        params += "connection.hosts" -> fenodes
      } catch { case _: Exception => }
      
      try {
        val user = extractValue[String](dorisRelation, "user")
        params += "auth.username" -> user
      } catch { case _: Exception => }
      
      try {
        val tableIdentifier = extractValue[String](dorisRelation, "tableIdentifier")
        params += "table.identifier" -> tableIdentifier
      } catch { case _: Exception => }
      
      params.toMap
    } catch {
      case _: Exception => Map.empty
    }
  }

  /**
   * Extracts connection parameters from Doris write command options.
   * 
   * @param options the command options map
   * @return Map of standardized connection parameters
   */
  private def extractDorisWriteParams(options: Map[String, String]): Map[String, Any] = {
    val params = scala.collection.mutable.Map[String, Any]()
    
    // Map Doris-specific options to standardized parameter names
    options.get("fenodes").foreach(params += "connection.hosts" -> _)
    options.get("user").foreach(params += "auth.username" -> _)
    options.get("password").foreach(params += "auth.password" -> _)
    options.get("doris.table.identifier").foreach(params += "table.identifier" -> _)
    options.get("doris.write.fields").foreach(params += "write.fields" -> _)
    options.get("doris.batch.size").foreach(params += "batch.size" -> _)
    options.get("doris.exec.mem.limit").foreach(params += "memory.limit" -> _)
    
    // Include any additional Doris-specific options
    options.filter(_._1.startsWith("doris.")).foreach { case (key, value) =>
      params += key -> value
    }
    
    params.toMap
  }

  /**
   * Validates and normalizes database and table names for Doris.
   * 
   * @param database the raw database name
   * @param table the raw table name
   * @return tuple of (normalized_database, normalized_table)
   */
  private def normalizeDorisIdentifiers(database: String, table: String): (String, String) = {
    val normalizedDb = Option(database).filter(_.nonEmpty).getOrElse("default")
    val normalizedTable = Option(table).filter(_.nonEmpty).getOrElse("unknown")
    (normalizedDb, normalizedTable)
  }

  /**
   * Generates a standardized source identifier for Doris tables.
   * 
   * @param database the Doris database name
   * @param table the Doris table name
   * @return SourceIdentifier with format "doris://database/table"
   */
  private def asSourceId(database: String, table: String): SourceIdentifier = {
    val (normalizedDb, normalizedTable) = normalizeDorisIdentifiers(database, table)
    SourceIdentifier(Some("doris"), s"doris://$normalizedDb/$normalizedTable")
  }

  /**
   * Checks if the given relation provider is a Doris data source.
   * 
   * @param provider the relation provider to check
   * @return true if it's a Doris provider
   */
  private def isDorisProvider(provider: AnyRef): Boolean = {
    provider match {
      case "doris" => true
      case "org.apache.doris.spark.sql.DefaultSource" => true
      case DorisDataSourceExtractor(_) => true
      case _ => 
        // Additional check for class name string matching
        provider.toString.contains("doris") && provider.toString.contains("DefaultSource")
    }
  }
}