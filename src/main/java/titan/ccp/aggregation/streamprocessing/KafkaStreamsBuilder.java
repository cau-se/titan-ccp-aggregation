package titan.ccp.aggregation.streamprocessing;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.common.kafka.streams.PropertiesBuilder;

/**
 * Builder for the Kafka Streams configuration.
 */
public class KafkaStreamsBuilder { // NOPMD builder methods

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

  private String applicationName; // NOPMD
  private String bootstrapServers; // NOPMD
  private String inputTopic; // NOPMD
  private String configurationTopic; // NOPMD
  private String feedbackTopic; // NOPMD
  private String outputTopic; // NOPMD
  private String schemaRegistryUrl; // NOPMD
  private Duration emitPeriod; // NOPMD
  private Duration gracePeriod; // NOPMD
  private int numThreads = -1; // NOPMD
  private int commitIntervalMs = -1; // NOPMD
  private int cacheMaxBytesBuff = -1; // NOPMD

  public KafkaStreamsBuilder applicationName(final String applicationName) {
    this.applicationName = applicationName;
    return this;
  }

  public KafkaStreamsBuilder inputTopic(final String inputTopic) {
    this.inputTopic = inputTopic;
    return this;
  }

  public KafkaStreamsBuilder configurationTopic(final String configurationTopic) {
    this.configurationTopic = configurationTopic;
    return this;
  }

  public KafkaStreamsBuilder feedbackTopic(final String feedbackTopic) {
    this.feedbackTopic = feedbackTopic;
    return this;
  }

  public KafkaStreamsBuilder outputTopic(final String outputTopic) {
    this.outputTopic = outputTopic;
    return this;
  }

  public KafkaStreamsBuilder windowSize(final Duration windowSize) {
    this.emitPeriod = Objects.requireNonNull(windowSize);
    return this;
  }

  public KafkaStreamsBuilder gracePeriod(final Duration gracePeriod) {
    this.gracePeriod = Objects.requireNonNull(gracePeriod);
    return this;
  }

  public KafkaStreamsBuilder schemaRegistry(final String url) {
    this.schemaRegistryUrl = url;
    return this;
  }

  public KafkaStreamsBuilder bootstrapServers(final String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
    return this;
  }

  /**
   * Sets the Kafka Streams property for the number of threads (num.stream.threads). Can be minus
   * one for using the default.
   */
  public KafkaStreamsBuilder numThreads(final int numThreads) {
    if (numThreads < -1 || numThreads == 0) {
      throw new IllegalArgumentException("Number of threads must be greater 0 or -1.");
    }
    this.numThreads = numThreads;
    return this;
  }

  /**
   * Sets the Kafka Streams property for the frequency with which to save the position (offsets in
   * source topics) of tasks (commit.interval.ms). Must be zero for processing all record, for
   * example, when processing bulks of records. Can be minus one for using the default.
   */
  public KafkaStreamsBuilder commitIntervalMs(final int commitIntervalMs) {
    if (commitIntervalMs < -1) {
      throw new IllegalArgumentException("Commit interval must be greater or equal -1.");
    }
    this.commitIntervalMs = commitIntervalMs;
    return this;
  }

  /**
   * Sets the Kafka Streams property for maximum number of memory bytes to be used for record caches
   * across all threads (cache.max.bytes.buffering). Must be zero for processing all record, for
   * example, when processing bulks of records. Can be minus one for using the default.
   */
  public KafkaStreamsBuilder cacheMaxBytesBuffering(final int cacheMaxBytesBuffering) {
    if (cacheMaxBytesBuffering < -1) {
      throw new IllegalArgumentException("Cache max bytes buffering must be greater or equal -1.");
    }
    this.cacheMaxBytesBuff = cacheMaxBytesBuffering;
    return this;
  }

  /**
   * Builds the {@link KafkaStreams} instance.
   */
  public KafkaStreams build() {
    Objects.requireNonNull(this.inputTopic, "Input topic has not been set.");
    Objects.requireNonNull(this.outputTopic, "Output topic has not been set.");
    Objects.requireNonNull(this.configurationTopic, "Configuration topic has not been set.");
    Objects.requireNonNull(this.schemaRegistryUrl, "Schema registry has not been set.");
    Objects.requireNonNull(this.emitPeriod, "Emit period has not been set.");
    Objects.requireNonNull(this.gracePeriod, "Grace period has not been set.");
    LOGGER.info(
        "Build Kafka Streams aggregation topology with topics: input='{}', 'configuration='{}', feedback='{}', output='{}'", // NOCS
        this.inputTopic, this.configurationTopic, this.feedbackTopic, this.outputTopic);
    LOGGER.info("Use Schema Registry at '{}'", this.schemaRegistryUrl);
    LOGGER.info(
        "Configure aggregation topologie with window parameters: windowSize='{}', gracePeriod='{}'",
        this.emitPeriod, this.gracePeriod);

    final TopologyBuilder topologyBuilder = new TopologyBuilder(
        new Serdes(this.schemaRegistryUrl),
        this.inputTopic,
        this.configurationTopic,
        this.feedbackTopic,
        this.outputTopic,
        this.emitPeriod,
        this.gracePeriod);

    // Non-null checks are performed by PropertiesBuilder
    final Properties properties = PropertiesBuilder
        .bootstrapServers(this.bootstrapServers)
        .applicationId(this.applicationName)
        .set(StreamsConfig.NUM_STREAM_THREADS_CONFIG, this.numThreads, p -> p > 0)
        .set(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, this.commitIntervalMs, p -> p >= 0)
        .set(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, this.cacheMaxBytesBuff, p -> p >= 0)
        .build();

    return new KafkaStreams(topologyBuilder.build(), properties);
  }

}
