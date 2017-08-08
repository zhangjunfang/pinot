package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LiKafkaConsumerWrapper implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerWrapper.class);

    private final String _clientId;
    private final boolean _metadataOnlyConsumer;
    private final String _topic;
    private final int _partition;
    private final long _connectTimeoutMillis;
    private final IFactory _simpleConsumerFactory;
    private String[] _bootstrapHosts;
    private int[] _bootstrapPorts;
    private IConsumer _simpleConsumer;
    private final Random _random = new Random();
    private KafkaBrokerWrapper _leader;
    private String _currentHost;
    private int _currentPort;

    /**
     * A Kafka protocol error that indicates a situation that is not likely to clear up by retrying the request (for
     * example, no such topic or offset out of range).
     */
    public static class PermanentConsumerException extends RuntimeException {
      public PermanentConsumerException(Errors error) {
        super(error.exception());
      }
    }

    /**
     * A Kafka protocol error that indicates a situation that is likely to be transient (for example, network error or
     * broker not available).
     */
    public static class TransientConsumerException extends RuntimeException {
      public TransientConsumerException(Errors error) {
        super(error.exception());
      }
    }

    private LiKafkaConsumerWrapper(IFactory simpleConsumerFactory, String bootstrapNodes,
        String clientId, long connectTimeoutMillis) {
      _simpleConsumerFactory = simpleConsumerFactory;
      _clientId = clientId;
      _connectTimeoutMillis = connectTimeoutMillis;
      _metadataOnlyConsumer = true;
      _simpleConsumer = null;

      // Topic and partition are ignored for metadata-only consumers
      _topic = null;
      _partition = Integer.MIN_VALUE;
    }

    private LiKafkaConsumerWrapper(IFactory simpleConsumerFactory, String bootstrapNodes,
        String clientId, String topic, int partition, long connectTimeoutMillis) {
      _simpleConsumerFactory = simpleConsumerFactory;
      _clientId = clientId;
      _topic = topic;
      _partition = partition;
      _connectTimeoutMillis = connectTimeoutMillis;
      _metadataOnlyConsumer = false;
      _simpleConsumer = null;
    }

    public synchronized int getPartitionCount(String topic, long timeoutMillis) {
      int unknownTopicReplyCount = 0;
      final int MAX_UNKNOWN_TOPIC_REPLY_COUNT = 10;
      int kafkaErrorCount = 0;
      final int MAX_KAFKA_ERROR_COUNT = 10;

      final long endTime = System.currentTimeMillis() + timeoutMillis;

      while(System.currentTimeMillis() < endTime) {

        // Send the metadata request to Kafka
        TopicMetadataResponse topicMetadataResponse = null;
        topicMetadataResponse = _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(topic)));


        final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
        final short errorCode = topicMetadata.errorCode();

        if (errorCode == Errors.NONE.code()) {
          return topicMetadata.partitionsMetadata().size();
        } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
          // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } else if (errorCode == Errors.INVALID_TOPIC_EXCEPTION.code()) {
          throw new RuntimeException("Invalid topic name " + topic);
        } else if (errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
          if (MAX_UNKNOWN_TOPIC_REPLY_COUNT < unknownTopicReplyCount) {
            throw new RuntimeException("Topic " + topic + " does not exist");
          } else {
            // Kafka topic creation can sometimes take some time, so we'll retry after a little bit
            unknownTopicReplyCount++;
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
          }
        } else {
          // Retry after a short delay
          kafkaErrorCount++;

          if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
            throw exceptionForKafkaErrorCode(errorCode);
          }

          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      }

      throw new TimeoutException();
    }

    private RuntimeException exceptionForKafkaErrorCode(short kafkaErrorCode) {
      final Errors kafkaError = Errors.forCode(kafkaErrorCode);
      switch (kafkaError) {
        case UNKNOWN:
        case OFFSET_OUT_OF_RANGE:
        case CORRUPT_MESSAGE:
        case MESSAGE_TOO_LARGE:
        case OFFSET_METADATA_TOO_LARGE:
        case INVALID_TOPIC_EXCEPTION:
        case RECORD_LIST_TOO_LARGE:
        case UNKNOWN_TOPIC_OR_PARTITION:
        case LEADER_NOT_AVAILABLE:
        case NOT_LEADER_FOR_PARTITION:
        case REQUEST_TIMED_OUT:
        case NETWORK_EXCEPTION:
        case NOT_ENOUGH_REPLICAS:
        case NOT_ENOUGH_REPLICAS_AFTER_APPEND:
        case NONE:
        default:
          return new RuntimeException("Unhandled error " + kafkaError);
      }
    }

    /**
     * Fetches the numeric Kafka offset for this partition for a symbolic name ("largest" or "smallest").
     *
     * @param requestedOffset Either "largest" or "smallest"
     * @param timeoutMillis Timeout in milliseconds
     * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
     * milliseconds
     * @return An offset
     */
    public synchronized long fetchPartitionOffset(String requestedOffset, int timeoutMillis)
        throws java.util.concurrent.TimeoutException {
      Preconditions.checkNotNull(requestedOffset);

      final long offsetRequestTime;
      if (requestedOffset.equalsIgnoreCase("largest")) {
        offsetRequestTime = kafka.api.OffsetRequest.LatestTime();
      } else if (requestedOffset.equalsIgnoreCase("smallest")) {
        offsetRequestTime = kafka.api.OffsetRequest.EarliestTime();
      } else if (requestedOffset.equalsIgnoreCase("testDummy")) {
        return -1L;
      } else {
        throw new IllegalArgumentException("Unknown initial offset value " + requestedOffset);
      }

      int kafkaErrorCount = 0;
      final int MAX_KAFKA_ERROR_COUNT = 10;

      final long endTime = System.currentTimeMillis() + timeoutMillis;

      while(System.currentTimeMillis() < endTime) {

        // Send the offset request to Kafka
        OffsetRequest request = new OffsetRequest(Collections.singletonMap(new TopicAndPartition(_topic, _partition),
            new PartitionOffsetRequestInfo(offsetRequestTime, 1)), kafka.api.OffsetRequest.CurrentVersion(), _clientId);
        OffsetResponse offsetResponse;
              offsetResponse = _simpleConsumer.getOffsetsBefore(request);

        final short errorCode = offsetResponse.errorCode(_topic, _partition);

        if (errorCode == Errors.NONE.code()) {
          long offset = offsetResponse.offsets(_topic, _partition)[0];
          if (offset == 0L) {
            LOGGER.warn("Fetched offset of 0 for topic {} and partition {}, is this a newly created topic?", _topic,
                _partition);
          }
          return offset;
        } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
          // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        } else {
          // Retry after a short delay
          kafkaErrorCount++;

          if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
            throw exceptionForKafkaErrorCode(errorCode);
          }

          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      }

      throw new TimeoutException();
    }

    private Iterable<MessageAndOffset> buildOffsetFilteringIterable(final ByteBufferMessageSet messageAndOffsets, final long startOffset, final long endOffset) {
      return Iterables.filter(messageAndOffsets, new Predicate<MessageAndOffset>() {
        @Override
        public boolean apply(@Nullable MessageAndOffset input) {
          // Filter messages that are either null or have an offset ∉ [startOffset; endOffset[
          if (input == null || input.offset() < startOffset || (endOffset <= input.offset() && endOffset != -1)) {
            return false;
          }

          // Check the message's checksum
          // TODO We might want to have better handling of this situation, maybe try to fetch the message again?
          if (!input.message().isValid()) {
            LOGGER.warn("Discarded message with invalid checksum in partition {} of topic {}", _partition, _topic);
            return false;
          }

          return true;
        }
      });
    }

    /**
     * Creates a simple consumer wrapper that connects to a random Kafka broker, which allows for fetching topic and
     * partition metadata. It does not allow to consume from a partition, since Kafka requires connecting to the
     * leader of that partition for consumption.
     *
     * @param simpleConsumerFactory The SimpleConsumer factory to use
     * @param bootstrapNodes A comma separated list of Kafka broker nodes
     * @param clientId The Kafka client identifier, to be used to uniquely identify the client when tracing calls
     * @param connectTimeoutMillis The timeout for connecting or re-establishing a connection to the Kafka cluster
     * @return A consumer wrapper
     */
    public static SimpleConsumerWrapper forMetadataConsumption(KafkaSimpleConsumerFactory simpleConsumerFactory,
        String bootstrapNodes, String clientId, long connectTimeoutMillis) {
      return new SimpleConsumerWrapper(simpleConsumerFactory, bootstrapNodes, clientId, connectTimeoutMillis);
    }

    /**
     * Creates a simple consumer wrapper that automatically connects to the leader broker for the given topic and
     * partition. This consumer wrapper can also fetch topic and partition metadata.
     *
     * @param simpleConsumerFactory The SimpleConsumer factory to use
     * @param bootstrapNodes A comma separated list of Kafka broker nodes
     * @param clientId The Kafka client identifier, to be used to uniquely identify the client when tracing calls
     * @param topic The Kafka topic to consume from
     * @param partition The partition id to consume from
     * @param connectTimeoutMillis The timeout for connecting or re-establishing a connection to the Kafka cluster
     * @return A consumer wrapper
     */
    public static SimpleConsumerWrapper forPartitionConsumption(KafkaSimpleConsumerFactory simpleConsumerFactory,
        String bootstrapNodes, String clientId, String topic, int partition, long connectTimeoutMillis) {
      return new SimpleConsumerWrapper(simpleConsumerFactory, bootstrapNodes, clientId, topic, partition,
          connectTimeoutMillis);
    }

    @Override
    /**
     * Closes this consumer.
     */
    public void close() throws IOException {
        _simpleConsumer.close();
        _simpleConsumer = null;
    }
}
