package titan.ccp.aggregation.streamprocessing;

import java.util.Properties;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for constructing and logging Kafka Stream {@link Properties}.
 */
public class PropertiesBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamsBuilder.class);

  private final Properties properties = new Properties();

  /**
   * Set the provided configuration key to the provided value.
   */
  public <T> PropertiesBuilder set(final String configKey, final T value) {
    this.properties.put(configKey, value);
    LOGGER.info("Set Kafka Streams configuration parameter '{}' to '{}'.", configKey, value);
    return this;
  }

  /**
   * Set the provided configuration key to the provided value if a given condition is evaluated to
   * true.
   */
  public <T> PropertiesBuilder set(final String configKey, final T value,
      final Predicate<T> condition) {
    if (condition.test(value)) {
      this.set(configKey, value);
    }
    return this;
  }

  public Properties build() {
    return this.properties;
  }

}
