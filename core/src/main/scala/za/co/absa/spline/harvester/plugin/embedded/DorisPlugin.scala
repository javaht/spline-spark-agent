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
import za.co.absa.spline.commons.reflect.ReflectionUtils.extractValue
import za.co.absa.spline.commons.reflect.extractors.SafeTypeMatchingExtractor
import za.co.absa.spline.harvester.builder.SourceIdentifier
import za.co.absa.spline.harvester.plugin.Plugin.{Precedence, ReadNodeInfo, WriteNodeInfo}
import za.co.absa.spline.harvester.plugin.embedded.DorisPlugin._
import za.co.absa.spline.harvester.plugin.{BaseRelationProcessing, DataSourceFormatNameResolving, Plugin, RelationProviderProcessing}

import javax.annotation.Priority

@Priority(Precedence.Normal)
class DorisPlugin
  extends Plugin
    with BaseRelationProcessing
    with RelationProviderProcessing
    with DataSourceFormatNameResolving {

  override def baseRelationProcessor: PartialFunction[(BaseRelation, LogicalRelation), ReadNodeInfo] = {
    case (`_: DorisRelation`(dr), _) =>
      val options = extractValue[Map[String, String]](dr, "options")
      val sourceId = asSourceId(options)
      ReadNodeInfo(sourceId, options)
  }

  override def relationProviderProcessor: PartialFunction[(AnyRef, SaveIntoDataSourceCommand), WriteNodeInfo] = {
    case (rp, cmd) if isDorisProvider(rp) =>
      val sourceId = asSourceId(cmd.options)
      WriteNodeInfo(sourceId, cmd.mode, cmd.query, cmd.options)
  }

  override def formatNameResolver: PartialFunction[AnyRef, String] = {
    case rp if isDorisProvider(rp) => "doris"
  }

  private def isDorisProvider(provider: AnyRef): Boolean = {
    provider match {
      case "doris" => true
      case "org.apache.doris.spark.sql.sources.DorisSource" => true
      case `_: DorisSource`(_) => true
      case _ => false
    }
  }

  private def asSourceId(options: Map[String, String]): SourceIdentifier = {


    val feNodes = options.getOrElse("doris.fenodes",
      sys.error("Doris: Cannot extract FE nodes from options. 'doris.fenodes' is required."))

    val tableIdentifier = options.getOrElse("doris.table.identifier",
      sys.error("Doris: 'doris.table.identifier' is required."))

    val parts = tableIdentifier.split("\\.")
    if (parts.length != 2) {
      sys.error("Doris: 'doris.table.identifier' should be in format 'database.table'")
    }

    val database = parts(0)
    val table = parts(1)

    val uri = s"doris://$feNodes/$database/$table"
    SourceIdentifier(Some("doris"), uri)
  }
}

object DorisPlugin {

  private object `_: DorisRelation` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.doris.spark.sql.sources.DorisRelation"
  )

  private object `_: DorisSource` extends SafeTypeMatchingExtractor[AnyRef](
    "org.apache.doris.spark.sql.sources.DorisSource"
  )
}
