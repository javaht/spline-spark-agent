
/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.spline.harvester.dispatcher.openmedatalineage

import org.apache.commons.configuration.Configuration
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.dispatcher.openmedatalineage.OpenmetadataLineageDispatcherConfig.{database_ServiceNames, host_Port, jwt_Token, pipeline_Description, pipeline_Name, pipeline_ServiceName, pipeline_SourceUrl}

object OpenmetadataLineageDispatcherConfig {
  val host_Port = "hostPort"
  val jwt_Token = "jwtToken"
  val pipeline_Name = "pipelineName"

  val pipeline_SourceUrl="pipelineSourceUrl"
  val pipeline_ServiceName="pipelineServiceName"
  val database_ServiceNames="databaseServiceNames"
  val pipeline_Description="pipelineDescription"


  def apply(c: Configuration) = new OpenmetadataLineageDispatcherConfig(c)
}

class OpenmetadataLineageDispatcherConfig(config: Configuration) {

  val hostPort: String = config.getRequiredString(host_Port)
  var jwtToken: String = getJwtToken(jwt_Token)
  val pipelineName: String = config.getRequiredString(pipeline_Name)

  val pipelineSourceUrl: String = config.getRequiredString(pipeline_SourceUrl)
  val pipelineServiceName: String = config.getRequiredString(pipeline_ServiceName)
  val databaseServiceNames: String = config.getRequiredString(database_ServiceNames)
  val pipelineDescription: String = config.getRequiredString(pipeline_Description)


  def getJwtToken(jwtToken: String): String = {
    String.format("Bearer %s", jwtToken)
  }


}
