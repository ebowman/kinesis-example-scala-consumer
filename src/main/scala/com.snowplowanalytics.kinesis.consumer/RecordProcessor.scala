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

import java.util

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.Record

import scala.collection.JavaConversions._
import scala.util.control.Breaks._

class RecordProcessorFactory(config: ConsumerConfig) extends IRecordProcessorFactory {
  override def createProcessor: IRecordProcessor = new RecordProcessor(config)
}

class RecordProcessor(config: ConsumerConfig) extends IRecordProcessor {

  private var kinesisShardId: String = _
  private var nextCheckpointTimeInMillis: Long = _

  // Backoff and retry settings.
  private val BACKOFF_TIME_IN_MILLIS = 3000L
  private val NUM_RETRIES = 10
  private val CHECKPOINT_INTERVAL_MILLIS = 1000L

  override def initialize(shardId: String) = {
    println("Initializing record processor for shard: " + shardId)
    this.kinesisShardId = shardId
  }

  override def processRecords(records: util.List[Record], checkpointer: IRecordProcessorCheckpointer) = {
    println(s"Processing ${records.size} records from $kinesisShardId")
    processRecordsWithRetries(records)

    if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
      checkpoint(checkpointer)
      nextCheckpointTimeInMillis = System.currentTimeMillis + CHECKPOINT_INTERVAL_MILLIS
    }
  }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason) = {
    println(s"Shutting down record processor for shard: $kinesisShardId")
    if (reason == ShutdownReason.TERMINATE) {
      checkpoint(checkpointer)
    }
  }

  private def printData(data: Array[Byte]) = println("data: " + new String(data))

  private def processRecordsWithRetries(records: util.List[Record]) = {
    for (record <- records) {
      try {
        println(s"Sequence number: ${record.getSequenceNumber}")
        printData(record.getData.array)
        println(s"Partition key: ${record.getPartitionKey}")
      } catch {
        case t: Throwable =>
          println(s"Caught throwable while processing record $record")
          println(t)
      }
    }
  }

  private def checkpoint(checkpointer: IRecordProcessorCheckpointer) = {
    println(s"Checkpointing shard $kinesisShardId")
    breakable {
      for (i <- 0 to NUM_RETRIES - 1) {
        try {
          checkpointer.checkpoint()
          break()
        } catch {
          case se: ShutdownException =>
            println("Caught shutdown exception, skipping checkpoint.", se)
          case e: ThrottlingException =>
            if (i >= (NUM_RETRIES - 1)) {
              println(s"Checkpoint failed after ${i + 1} attempts.", e)
            } else {
              println(s"Transient issue when checkpointing - attempt ${i + 1} of $NUM_RETRIES", e)
            }
          case e: InvalidStateException =>
            println("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e)
        }
        Thread.sleep(BACKOFF_TIME_IN_MILLIS)
      }
    }
  }
}
