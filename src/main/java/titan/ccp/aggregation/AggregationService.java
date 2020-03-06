package titan.ccp.aggregation;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.aggregation.streamprocessing.KafkaStreamsBuilder;
import titan.ccp.aggregation.streamprocessing.TopologyBuilder;
import titan.ccp.common.configuration.ServiceConfigurations;
import titan.ccp.common.kafka.utils.AdminUtils;

/**
 * A microservice that manages the history and, therefore, stores and aggregates incoming
 * measurements.
 *
 */
public class AggregationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  private static final String NAME = "titan-ccp-aggregation";
  private static final String VERSION = "0.0.3";

  private final Configuration config = ServiceConfigurations.createWithDefaults();
  private final CompletableFuture<Void> stopEvent = new CompletableFuture<>();

  /**
   * Start the service.
   */
  public void run() {
    this.createKafkaStreamsApplication();
  }

  /**
   * Stop the service.
   */
  public void stop() {
    this.stopEvent.complete(null);
  }

  /**
   * Build and start the underlying Kafka Streams Application of the service.
   *
   * @param clusterSession the database session which the application should use.
   */
  private void createKafkaStreamsApplication() {
    final String servers = this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS);
    final String inputTopic = this.config.getString(ConfigurationKeys.KAFKA_INPUT_TOPIC);
    final String configTopic = this.config.getString(ConfigurationKeys.KAFKA_CONFIGURATION_TOPIC);
    final String feedbackTopic = this.config.getString(ConfigurationKeys.KAFKA_FEEDBACK_TOPIC);
    final String outputTopic = this.config.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC);

    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .applicationName(NAME + '-' + VERSION)
        .bootstrapServers(this.config.getString(ConfigurationKeys.KAFKA_BOOTSTRAP_SERVERS))
        .inputTopic(inputTopic)
        .configurationTopic(configTopic)
        .feedbackTopic(feedbackTopic)
        .outputTopic(outputTopic)
        .schemaRegistry(this.config.getString(ConfigurationKeys.SCHEMA_REGISTRY_URL))
        .windowSize(Duration.ofMillis(this.config.getLong(ConfigurationKeys.EMIT_PERIOD_MS)))
        .gracePeriod(Duration.ofMillis(this.config.getLong(ConfigurationKeys.WINDOW_GRACE_MS)))
        .numThreads(this.config.getInt(ConfigurationKeys.NUM_THREADS))
        .commitIntervalMs(this.config.getInt(ConfigurationKeys.COMMIT_INTERVAL_MS))
        .cacheMaxBytesBuffering(this.config.getInt(ConfigurationKeys.CACHE_MAX_BYTES_BUFFERING))
        .build();

    try (AdminUtils kafkaUtils = AdminUtils.fromBootstrapServers(servers)) {
      kafkaUtils.awaitTopicsExists(Set.of(inputTopic, outputTopic, configTopic))
          .exceptionally(t -> {
            LOGGER.error("Required Kafka topics do not exist.", t);
            return null;
          })
          .join();
    } catch (final Exception e) { // NOPMD generic exception is thrown
      LOGGER.error("An error occured while closing the AdminUtils.", e);
    }

    this.stopEvent.thenRun(kafkaStreams::close);

    kafkaStreams.start();
  }



  public static void main(final String[] args) {
    new AggregationService().run();
  }

}
