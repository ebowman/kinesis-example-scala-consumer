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

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import com.amazonaws.services.kinesis.model.{Record, Shard}
import com.typesafe.config.ConfigFactory

import scala.concurrent.Future

case class Drain(poller: ConsumerPoller, streamName: String, shard: Shard)

case class Next(shard: Shard, records: Stream[Future[Seq[Record]]])

class PollActor extends Actor with ActorLogging {
  override def receive = {
    case Next(shard, stream) =>
      implicit val ec = context.dispatcher
      if (stream.nonEmpty) {
        val f = stream.head
        f.map { seq =>
          seq.foreach { record =>
            println(s"Streaming ${record.getData.array().size} bytes: ${new String(record.getData.array())}")
          }
          import scala.concurrent.duration._
          if (seq.nonEmpty) {
            context.system.scheduler.scheduleOnce(1.second, self, Next(shard, stream.tail))
          }
        }.onFailure {
          case ex => ex.printStackTrace()
        }
      } else {
        println("Drained")
      }

    case Drain(poller, streamName, shard) =>
      implicit val ec = context.dispatcher
      println(s"Drain($poller, $streamName, $shard")
      val result: Future[Stream[Future[Seq[Record]]]] = poller.drain(streamName, shard, 10, 5, None)
      result.onFailure {
        case ex => ex.printStackTrace()
      }
      result.map(self ! Next(shard, _))
  }
}

object ConsumerApp extends App {
  val config = new ConsumerConfig(ConfigFactory.parseResources("default.conf"))
  if (true) {
    import scala.concurrent.ExecutionContext.Implicits.global
    val poller = new ConsumerPoller(config)

    val system = ActorSystem("ConsumerPoller")
    val actor = system.actorOf(Props[PollActor])

    poller.containsStream(config.streamName) map {
      case true =>
        def recurse(shards: Stream[Future[Seq[Shard]]]) {
          if (shards.nonEmpty) {
            shards.head.map { seq =>
              seq.foreach {
                shard =>
                  import scala.concurrent.duration._
                  system.scheduler.schedule(0.seconds, 10.seconds, actor, Drain(poller, config.streamName, shard))
              }
              if (seq.nonEmpty) recurse(shards.tail)
            }.onFailure {
              case ex => ex.printStackTrace()
            }
          }
        }
        recurse(poller.shards(config.streamName))
      case false =>
        sys.error(s"Could not find stream ${config.streamName}")
    }

    val nameFutures: Stream[Future[Seq[String]]] = poller.streamNames()

    def containsStream(futures: Stream[Future[Seq[String]]]): Future[Boolean] = {
      futures.head.flatMap {
        seq =>
          if (seq.contains(config.streamName)) Future.successful(true)
          else containsStream(futures.tail)
      }
    }

    containsStream(poller.streamNames()) map {
      case true =>
        println(s"Found stream ${config.streamName}")
      case false =>

    }

    this.synchronized(this.wait())

    //poller.batch()

  } else {
    new ConsumerWorker(config)
  }
}

