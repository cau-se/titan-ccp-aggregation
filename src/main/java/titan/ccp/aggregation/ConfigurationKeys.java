package titan.ccp.aggregation;

/**
 * Keys to access configuration parameters.
 */
public final class ConfigurationKeys {

  public static final String APPLICATION_NAME = "application.name";

  public static final String APPLICATION_VERSION = "application.version";

  public static final String KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";

  public static final String KAFKA_INPUT_TOPIC = "kafka.input.topic";

  public static final String KAFKA_CONFIGURATION_TOPIC = "kafka.configuration.topic";

  public static final String KAFKA_FEEDBACK_TOPIC = "kafka.feedback.topic";

  public static final String KAFKA_OUTPUT_TOPIC = "kafka.output.topic";

  public static final String EMIT_PERIOD_MS = "emit.period.ms";

  public static final String WINDOW_GRACE_MS = "grace.period.ms";

  public static final String NUM_THREADS = "num.threads";

  public static final String COMMIT_INTERVAL_MS = "commit.interval.ms";

  public static final String CACHE_MAX_BYTES_BUFFERING = "cache.max.bytes.buffering";

  public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

  private ConfigurationKeys() {}

}
