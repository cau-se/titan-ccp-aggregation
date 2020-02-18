package titan.ccp.aggregation.streamprocessing;

import java.time.Duration;
import java.util.Set;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventSerde;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

/**
 * Builds Kafka Stream Topology for the History microservice.
 */
public class TopologyBuilder {

  // private static final Logger LOGGER = LoggerFactory.getLogger(TopologyBuilder.class);

  private static final String FEEDBACK_TOPIC_NAME_TEMP = "feedback";

  private final Serdes serdes;
  private final String inputTopic;
  private final String feedbackTopic = FEEDBACK_TOPIC_NAME_TEMP;
  private final String outputTopic;
  private final String configurationTopic;
  private final Duration windowSize;
  private final Duration gracePeriod;

  private final StreamsBuilder builder = new StreamsBuilder();
  private final RecordAggregator recordAggregator = new RecordAggregator();

  /**
   * Create a new {@link TopologyBuilder} using the given topics.
   *
   * @param serdes The serdes to use in the topology.
   * @param inputTopic The topic where to get the data from.
   * @param outputTopic The topic where to write the aggregated data.
   * @param configurationTopic The topic where the hierarchy of the sensors is published.
   */
  public TopologyBuilder(final Serdes serdes, final String inputTopic, final String outputTopic,
      final String configurationTopic, final Duration windowSize, final Duration gracePeriod) {
    this.serdes = serdes;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.configurationTopic = configurationTopic;
    this.windowSize = windowSize;
    this.gracePeriod = gracePeriod;
  }

  /**
   * Build the {@link Topology} for the History microservice.
   */
  public Topology build() {
    // 1. Build Parent-Sensor Table
    final KTable<String, Set<String>> parentSensorTable = this.buildParentSensorTable();

    // 2. Build Input Table
    final KTable<String, ActivePowerRecord> inputTable = this.buildInputTable();

    // 3. Build Last Value Table from Input and Parent-Sensor Table
    final KTable<Windowed<SensorParentKey>, ActivePowerRecord> lastValueTable =
        this.buildLastValueTable(parentSensorTable, inputTable);

    // 4. Build Aggregations Stream
    final KTable<Windowed<String>, AggregatedActivePowerRecord> aggregations =
        this.buildAggregationStream(lastValueTable);

    // 5. Expose Aggregations Stream
    this.exposeOutputStream(aggregations);

    return this.builder.build();
  }

  /**
   * Creates a table for the input topic and maps the Avro {@code ActivePowerRecord} to the Kieker
   * {@code ActivePowerRecord}.
   *
   * @return a table from the input topic
   */
  private KTable<String, ActivePowerRecord> buildInputTable() {
    final KStream<String, ActivePowerRecord> values = this.builder
        .stream(this.inputTopic, Consumed.with(
            this.serdes.string(),
            this.serdes.activePowerRecordValues()))
        .mapValues(apAvro -> new ActivePowerRecord(
            apAvro.getIdentifier(),
            apAvro.getTimestamp(),
            apAvro.getValueInW()));
    final KStream<String, ActivePowerRecord> aggregationsInput = this.builder
        .stream(this.feedbackTopic, Consumed.with(
            this.serdes.string(),
            this.serdes.aggregatedActivePowerRecordValues()))
        .mapValues(r -> new ActivePowerRecord(r.getIdentifier(), r.getTimestamp(), r.getSumInW()));

    final KTable<String, ActivePowerRecord> inputTable = values
        .merge(aggregationsInput)
        .groupByKey(Grouped.with(
            this.serdes.string(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .reduce((aggr, value) -> value, Materialized.with(
            this.serdes.string(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));
    return inputTable;
  }

  private KTable<String, Set<String>> buildParentSensorTable() {
    final KStream<Event, String> configurationStream = this.builder
        .stream(this.configurationTopic, Consumed.with(EventSerde.serde(), this.serdes.string()))
        .filter((key, value) -> key == Event.SENSOR_REGISTRY_CHANGED
            || key == Event.SENSOR_REGISTRY_STATUS);

    final ChildParentsTransformerFactory childParentsTransformerFactory =
        new ChildParentsTransformerFactory();
    this.builder.addStateStore(childParentsTransformerFactory.getStoreBuilder());

    return configurationStream
        .mapValues(data -> SensorRegistry.fromJson(data))
        .flatTransform(
            childParentsTransformerFactory.getTransformerSupplier(),
            childParentsTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(this.serdes.string(), OptionalParentsSerde.serde()))
        .aggregate(
            () -> Set.<String>of(),
            (key, newValue, oldValue) -> newValue.orElse(null),
            Materialized.with(this.serdes.string(), ParentsSerde.serde()));
  }

  private KTable<Windowed<SensorParentKey>, ActivePowerRecord> buildLastValueTable(
      final KTable<String, Set<String>> parentSensorTable,
      final KTable<String, ActivePowerRecord> inputTable) {
    final JointFlatTransformerFactory jointFlatMapTransformerFactory =
        new JointFlatTransformerFactory();
    this.builder.addStateStore(jointFlatMapTransformerFactory.getStoreBuilder());

    return inputTable
        .join(parentSensorTable, (record, parents) -> new JointRecordParents(parents, record))
        .toStream()
        .flatTransform(
            jointFlatMapTransformerFactory.getTransformerSupplier(),
            jointFlatMapTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(
            SensorParentKeySerde.serde(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .windowedBy(TimeWindows.of(this.windowSize).grace(this.gracePeriod))
        .reduce(
            // TODO Configurable window aggregation function
            (aggValue, newValue) -> newValue,
            Materialized.with(
                SensorParentKeySerde.serde(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));

  }

  private KTable<Windowed<String>, AggregatedActivePowerRecord> buildAggregationStream(
      final KTable<Windowed<SensorParentKey>, ActivePowerRecord> lastValueTable) {
    return lastValueTable
        .groupBy(
            (k, v) -> KeyValue.pair(new Windowed<>(k.key().getParent(), k.window()), v),
            Grouped.with(
                new WindowedSerdes.TimeWindowedSerde<>(
                    this.serdes.string(),
                    this.windowSize.toMillis()),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .aggregate(
            () -> null,
            this.recordAggregator::add,
            this.recordAggregator::substract,
            Materialized.with(
                new WindowedSerdes.TimeWindowedSerde<>(
                    this.serdes.string(),
                    this.windowSize.toMillis()),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        // TODO timestamp -1 indicates that this record is emitted by an substract event
        .filter((k, record) -> record.getTimestamp() != -1);
    // TODO compute Timestamp
    // .mapValues((k, v) -> new AggregatedActivePowerRecord(
    // v.getIdentifier(),
    // k.window().end() - 1,
    // v.getMinInW(),
    // v.getMaxInW(),
    // v.getCount(),
    // v.getSumInW(),
    // v.getAverageInW()));

    // .suppress(Suppressed.untilTimeLimit(this.windowSize, BufferConfig.unbounded()))
    // .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))


  }

  /**
   * Write the aggreations stream to the output topic. It converts the Kieker
   * {@code AggregatedActivePowerRecord} to the Avro {@code AggregatedActivePowerRecord}.
   *
   * @param aggregations containing the aggregations of the input data.
   */
  private void exposeOutputStream(
      final KTable<Windowed<String>, AggregatedActivePowerRecord> aggregations) {

    aggregations
        .toStream()
        .filter((k, record) -> record != null)
        .map((k, v) -> KeyValue.pair(
            k.key(),
            titan.ccp.model.records.AggregatedActivePowerRecord.newBuilder()
                .setIdentifier(v.getIdentifier())
                .setTimestamp(v.getTimestamp())
                .setMinInW(v.getMinInW())
                .setMaxInW(v.getMaxInW())
                .setCount(v.getCount())
                .setSumInW(v.getSumInW())
                .setAverageInW(v.getAverageInW())
                .build()))
        .to(this.feedbackTopic, Produced.with(
            this.serdes.string(), this.serdes.aggregatedActivePowerRecordValues()));

    aggregations
        .suppress(Suppressed.untilTimeLimit(this.windowSize, BufferConfig.unbounded()))
        .toStream()
        .map((k, v) -> KeyValue.pair(
            k.key(),
            titan.ccp.model.records.AggregatedActivePowerRecord.newBuilder()
                .setIdentifier(v.getIdentifier())
                .setTimestamp(v.getTimestamp())
                .setMinInW(v.getMinInW())
                .setMaxInW(v.getMaxInW())
                .setCount(v.getCount())
                .setSumInW(v.getSumInW())
                .setAverageInW(v.getAverageInW())
                .build()))
        .to(this.outputTopic, Produced.with(
            this.serdes.string(),
            this.serdes.aggregatedActivePowerRecordValues()));
  }

}
