package com.snowplowanalytics.kinesis.consumer

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model._

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}

object ConsumerPoller {

  implicit class Condenser[T](val v: Stream[Future[Stream[T]]]) extends AnyVal {
    def condense(implicit ec: ExecutionContext): Future[Stream[T]] = Future.sequence(v).map(_.flatten)
  }

}

class ConsumerPoller(config: ConsumerConfig)(implicit ec: ExecutionContext) {
  val client = new AmazonKinesisClient(config.credentials: AWSCredentials)
  client.setEndpoint(config.endpoint, "kinesis", config.region)

  def streamNames(limit: Int = 10): Stream[Future[Seq[String]]] = {
    val request = new ListStreamsRequest().withLimit(limit)
    @volatile var result: ListStreamsResult = null

    def firstRequest(): Future[Seq[String]] = Future {
      result = blocking(client.listStreams(request))
      result.getStreamNames.asScala
    }

    def nextRequest(): Future[Seq[String]] = Future {
      if (result.isHasMoreStreams) {
        val names = result.getStreamNames.asScala
        if (names.nonEmpty) {
          request.setExclusiveStartStreamName(names.last)
        }
        result = blocking(client.listStreams(request))
        result.getStreamNames.asScala
      } else {
        Stream.empty
      }
    }
    firstRequest() #:: Stream.continually(nextRequest())
  }

  def containsStream(streamName: String, limit: Int = 10): Future[Boolean] = {
    def contains(futures: Stream[Future[Seq[String]]]): Future[Boolean] = {
      if (futures.isEmpty) Future.successful(false)
      else {
        futures.head.flatMap {
          seq =>
            if (seq.contains(streamName)) Future.successful(true)
            else contains(futures.tail)
        }
      }
    }
    contains(streamNames(limit))
  }

  def shards(streamName: String): Stream[Future[Seq[Shard]]] = {
    val request = new DescribeStreamRequest().withStreamName(streamName).withExclusiveStartShardId(null)
    @volatile var result: DescribeStreamResult = null

    def firstRequest(): Future[Seq[Shard]] = Future {
      result = blocking(client.describeStream(request))
      result.getStreamDescription.getShards.asScala
    }

    def nextRequest(): Future[Seq[Shard]] = Future {
      if (result == null) {
        Nil
      } else {
        val prevShards = result.getStreamDescription.getShards.asScala
        if (result.getStreamDescription.getHasMoreShards && (prevShards != null && prevShards.nonEmpty)) {
          request.setExclusiveStartShardId(prevShards.last.getShardId)
          result = blocking(client.describeStream(request))
          result.getStreamDescription.getShards.asScala
        } else {
          result = null
          Nil
        }
      }
    }
    firstRequest() #:: Stream.continually(nextRequest())
  }

  def findShard(streamName: String, shardId: String): Future[Option[Shard]] = {
    val shrds: Stream[Future[Seq[Shard]]] = shards(streamName)

    def find(s: Stream[Future[Seq[Shard]]]): Future[Option[Shard]] = {
      if (s.isEmpty) Future.successful(None)
      else {
        s.head.flatMap {
          shards =>
            val result = shards.find(_.getShardId == shardId)
            if (result == None) find(s.tail)
            else Future.successful(result)
        }
      }
    }

    find(shards(streamName))
  }

  def shardIterator(streamName: String, shard: Shard, sequenceNumber: Option[String] = None): Future[String] = Future {
    val request = new GetShardIteratorRequest().withStreamName(streamName).withShardId(shard.getShardId)
    sequenceNumber match {
      case Some(seqNo) =>
        request.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
        request.setStartingSequenceNumber(seqNo)
      case None =>
        request.setShardIteratorType(ShardIteratorType.TRIM_HORIZON)
    }
    blocking(client.getShardIterator(request)).getShardIterator
  }

  def drain(streamName: String,
            shard: Shard,
            limit: Int,
            threshold: Int,
            sequenceNumber: Option[String] = None): Future[Stream[Future[Seq[Record]]]] = {

    shardIterator(streamName, shard, sequenceNumber) map { shardIterator =>
      val request = new GetRecordsRequest().withLimit(limit)
      @volatile var prevResult: GetRecordsResult = null

      def firstRequest(): Future[Stream[Record]] = Future {
        request.setShardIterator(shardIterator)
        prevResult = blocking(client.getRecords(request))
        println(s"firstRequest result = $prevResult")
        prevResult.getRecords.asScala.toStream
      }

      def nextRequest(): Future[Stream[Record]] = Future {
        val prevRecords = prevResult.getRecords.asScala
        if (prevRecords.size < threshold) {
          println("Done draining")
          Stream.empty
        } else {
          request.setShardIterator(prevResult.getNextShardIterator)
          prevResult = blocking(client.getRecords(request))
          println(s"nextRequest result = $prevResult")
          prevResult.getRecords.asScala.toStream
        }
      }

      firstRequest() #:: Stream.continually(nextRequest())
    }
  }
}
