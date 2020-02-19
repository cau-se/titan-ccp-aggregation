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
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventSerde;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.sensorregistry.SensorRegistry;

/**
 * Builds Kafka Stream Topology for the History microservice.
 */
public class TopologyBuilder {

  private final Serdes serdes;
  private final String inputTopic;
  private final String feedbackTopic;
  private final String outputTopic;
  private final String configurationTopic;
  private final Duration emitPeriod;
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
  public TopologyBuilder(final Serdes serdes, final String inputTopic,
      final String configurationTopic, final String feedbackTopic, final String outputTopic,
      final Duration emitPeriod, final Duration gracePeriod) {
    this.serdes = serdes;
    this.inputTopic = inputTopic;
    this.configurationTopic = configurationTopic;
    this.feedbackTopic = feedbackTopic;
    this.outputTopic = outputTopic;
    this.emitPeriod = emitPeriod;
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
            this.serdes.activePowerRecordValues()));
    final KStream<String, ActivePowerRecord> aggregationsInput = this.builder
        .stream(this.feedbackTopic, Consumed.with(
            this.serdes.string(),
            this.serdes.aggregatedActivePowerRecordValues()))
        .mapValues(r -> new ActivePowerRecord(r.getIdentifier(), r.getTimestamp(), r.getSumInW()));

    final KTable<String, ActivePowerRecord> inputTable = values
        .merge(aggregationsInput)
        .groupByKey(Grouped.with(
            this.serdes.string(),
            this.serdes.activePowerRecordValues()))
        .reduce((aggr, value) -> value, Materialized.with(
            this.serdes.string(),
            this.serdes.activePowerRecordValues()));
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
            this.serdes.activePowerRecordValues()))
        .windowedBy(TimeWindows.of(this.emitPeriod).grace(this.gracePeriod))
        .reduce(
            // TODO Configurable window aggregation function
            (oldVal, newVal) -> newVal.getTimestamp() >= oldVal.getTimestamp() ? newVal : oldVal,
            Materialized.with(
                SensorParentKeySerde.serde(),
                this.serdes.activePowerRecordValues()));

  }

  private KTable<Windowed<String>, AggregatedActivePowerRecord> buildAggregationStream(
      final KTable<Windowed<SensorParentKey>, ActivePowerRecord> lastValueTable) {
    return lastValueTable
        .groupBy(
            (k, v) -> KeyValue.pair(new Windowed<>(k.key().getParent(), k.window()), v),
            Grouped.with(
                new WindowedSerdes.TimeWindowedSerde<>(
                    this.serdes.string(),
                    this.emitPeriod.toMillis()),
                this.serdes.activePowerRecordValues()))
        .aggregate(
            () -> null,
            this.recordAggregator::add,
            this.recordAggregator::substract,
            Materialized.with(
                new WindowedSerdes.TimeWindowedSerde<>(
                    this.serdes.string(),
                    this.emitPeriod.toMillis()),
                this.serdes.aggregatedActivePowerRecordValues()))
        // TODO timestamp -1 indicates that this record is emitted by an substract event
        .filter((k, record) -> record.getTimestamp() != -1);
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
        .selectKey((k, v) -> k.key())
        .to(this.feedbackTopic, Produced.with(
            this.serdes.string(), this.serdes.aggregatedActivePowerRecordValues()));

    aggregations
        // .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .suppress(Suppressed.untilTimeLimit(this.emitPeriod, BufferConfig.unbounded()))
        .toStream()
        .selectKey((k, v) -> k.key())
        .to(this.outputTopic, Produced.with(
            this.serdes.string(),
            this.serdes.aggregatedActivePowerRecordValues()));
  }

}
