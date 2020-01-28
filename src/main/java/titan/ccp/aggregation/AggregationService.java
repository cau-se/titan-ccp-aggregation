package titan.ccp.aggregation;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.configuration2.Configuration;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import titan.ccp.aggregation.streamprocessing.KafkaStreamsBuilder;
import titan.ccp.aggregation.streamprocessing.TopologyBuilder;
import titan.ccp.common.configuration.Configurations;
import titan.ccp.common.kafka.utils.AdminUtils;

/**
 * A microservice that manages the history and, therefore, stores and aggregates incoming
 * measurements.
 *
 */
public class AggregationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  private final Configuration config = Configurations.create();
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
    final String outputTopic = this.config.getString(ConfigurationKeys.KAFKA_OUTPUT_TOPIC);
    final String configTopic = this.config.getString(ConfigurationKeys.CONFIGURATION_KAFKA_TOPIC);

    final KafkaStreams kafkaStreams = new KafkaStreamsBuilder()
        .bootstrapServers(servers)
        .inputTopic(inputTopic)
        .outputTopic(outputTopic)
        .configurationTopic(configTopic)
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
