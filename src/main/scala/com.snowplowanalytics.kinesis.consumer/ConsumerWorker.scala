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

import java.net.InetAddress
import java.util.UUID

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import com.amazonaws.services.kinesis.metrics.impl.NullMetricsFactory

class ConsumerWorker(config: ConsumerConfig) {
  val workerId = InetAddress.getLocalHost.getCanonicalHostName + ":" + UUID.randomUUID()
  println("Using workerId: " + workerId)

  import config._

  val kinesisClientLibConfiguration = new KinesisClientLibConfiguration(appName + util.Random.nextInt(), streamName, config.credentials, workerId
  ).withInitialPositionInStream(InitialPositionInStream.valueOf(initialPosition))

  println(s"Running: $appName.")
  println(s"Processing stream: $streamName")

  val recordProcessorFactory = new RecordProcessorFactory(config)
  val worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration, new NullMetricsFactory())

  worker.run()

}

