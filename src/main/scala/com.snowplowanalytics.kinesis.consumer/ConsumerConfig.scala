/*
* Copyright (c) 2013-2014 Snowplow Analytics Ltd. with significant
* portions copyright 2012-2014 Amazon.
* All rights reserved.
*
* This program is licensed to you under the Apache License Version 2.0,
* and you may not use this file except in compliance with the Apache
* License Version 2.0.
* You may obtain a copy of the Apache License Version 2.0 at
* http://www.apache.org/licenses/LICENSE-2.0.
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the Apache License Version 2.0 is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
* either express or implied.
*
* See the Apache License Version 2.0 for the specific language
* governing permissions and limitations there under.
*/

package com.snowplowanalytics.kinesis.consumer

import com.amazonaws.auth.{AWSCredentialsProvider, BasicAWSCredentials}
import com.typesafe.config.Config


case class Credentials(accessKey: String, secretKey: String) extends AWSCredentialsProvider {
  val getCredentials = new BasicAWSCredentials(accessKey, secretKey)

  def refresh() = {}
}

class ConsumerConfig(config: Config) {
  private val stream = config.resolve.getConfig("consumer").getConfig("stream")

  val streamName = stream.getString("stream-name")
  val appName = stream.getString("app-name")
  val initialPosition = stream.getString("initial-position")

  val accessKey = sys.env("AWS_ACCESS_KEY_ID")
  val secretKey = sys.env("AWS_SECRET_ACCESS_KEY")

  lazy val credentials = Credentials(accessKey, secretKey)
}


