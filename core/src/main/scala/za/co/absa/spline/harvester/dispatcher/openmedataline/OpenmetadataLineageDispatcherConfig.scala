
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

package za.co.absa.spline.harvester.dispatcher.openmedataline

import org.apache.commons.configuration.Configuration
import za.co.absa.spline.commons.config.ConfigurationImplicits._
import za.co.absa.spline.harvester.dispatcher.openmedataline.OpenmetadataLineageDispatcherConfig.{ApiUrlProperty, Token}
import scala.concurrent.duration._

object OpenmetadataLineageDispatcherConfig {
  val ApiUrlProperty = "api.url"
  val Token = "token"

  def apply(c: Configuration) = new OpenmetadataLineageDispatcherConfig(c)
}

class OpenmetadataLineageDispatcherConfig(config: Configuration) {
  val apiUrl: String = config.getRequiredString(ApiUrlProperty)
  val token: String = config.getRequiredString(Token)
}
