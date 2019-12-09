package titan.ccp.aggregation.streamprocessing;

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
import titan.ccp.common.kieker.kafka.IMonitoringRecordSerde;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventSerde;
import titan.ccp.model.records.AggregatedActivePowerRecord.Builder;
import titan.ccp.model.sensorregistry.SensorRegistry;
import titan.ccp.models.records.ActivePowerRecord;
import titan.ccp.models.records.ActivePowerRecordFactory;
import titan.ccp.models.records.AggregatedActivePowerRecord;
import titan.ccp.models.records.AggregatedActivePowerRecordFactory;

/**
 * Builds Kafka Stream Topology for the History microservice.
 */
public class TopologyBuilder {

  // private static final Logger LOGGER =
  // LoggerFactory.getLogger(TopologyBuilder.class);

  private final Serdes serdes;
  private final String inputTopic;
  private final String outputTopic;
  private final String configurationTopic;

  private final StreamsBuilder builder = new StreamsBuilder();
  private final RecordAggregator recordAggregator = new RecordAggregator();

  /**
   * Create a new {@link TopologyBuilder} using the given topics.
   *
   * @param serdes
   */
  public TopologyBuilder(final Serdes serdes, final String inputTopic, final String outputTopic,
      final String configurationTopic) {
    this.serdes = serdes;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.configurationTopic = configurationTopic;
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
    final KTable<SensorParentKey, ActivePowerRecord> lastValueTable =
        this.buildLastValueTable(parentSensorTable, inputTable);

    // 4. Build Aggregations Stream
    final KStream<String, AggregatedActivePowerRecord> aggregations =
        this.buildAggregationStream(lastValueTable);

    // 5. Expose Aggregations Stream
    this.exposeOutputStream(aggregations);

    return this.builder.build();
  }

  private KTable<String, ActivePowerRecord> buildInputTable() {
    return this.builder.table(this.inputTopic, Consumed.with(this.serdes.string(),
        IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));
  }

  private KTable<String, Set<String>> buildParentSensorTable() {
    final KStream<Event, String> configurationStream = this.builder
        .stream(this.configurationTopic, Consumed.with(EventSerde.serde(), this.serdes.string()))
        .filter((key, value) -> key == Event.SENSOR_REGISTRY_CHANGED
            || key == Event.SENSOR_REGISTRY_STATUS);

    final ChildParentsTransformerFactory childParentsTransformerFactory =
        new ChildParentsTransformerFactory();
    this.builder.addStateStore(childParentsTransformerFactory.getStoreBuilder());

    return configurationStream.mapValues(data -> SensorRegistry.fromJson(data))
        .flatTransform(childParentsTransformerFactory.getTransformerSupplier(),
            childParentsTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(this.serdes.string(), OptionalParentsSerde.serde()))
        .aggregate(() -> Set.<String>of(), (key, newValue, oldValue) -> newValue.orElse(null),
            Materialized.with(this.serdes.string(), ParentsSerde.serde()));
  }

  private KTable<SensorParentKey, ActivePowerRecord> buildLastValueTable(
      final KTable<String, Set<String>> parentSensorTable,
      final KTable<String, ActivePowerRecord> inputTable) {
    final JointFlatTransformerFactory jointFlatMapTransformerFactory =
        new JointFlatTransformerFactory();
    this.builder.addStateStore(jointFlatMapTransformerFactory.getStoreBuilder());

    return inputTable
        .join(parentSensorTable, (record, parents) -> new JointRecordParents(parents, record))
        .toStream()
        .flatTransform(jointFlatMapTransformerFactory.getTransformerSupplier(),
            jointFlatMapTransformerFactory.getStoreName())
        .groupByKey(Grouped.with(SensorParentKeySerde.serde(),
            IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .reduce(
            // TODO Also deduplicate here?
            (aggValue, newValue) -> newValue, Materialized.with(SensorParentKeySerde.serde(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())));
  }

  private KStream<String, AggregatedActivePowerRecord> buildAggregationStream(
      final KTable<SensorParentKey, ActivePowerRecord> lastValueTable) {
    return lastValueTable
        .groupBy((k, v) -> KeyValue.pair(k.getParent(), v),
            Grouped.with(this.serdes.string(),
                IMonitoringRecordSerde.serde(new ActivePowerRecordFactory())))
        .aggregate(() -> null, this.recordAggregator::add, this.recordAggregator::substract,
            Materialized.with(this.serdes.string(),
                IMonitoringRecordSerde.serde(new AggregatedActivePowerRecordFactory())))
        .toStream()
        // TODO TODO timestamp -1 indicates that this record is emitted by an substract
        // event
        .filter((k, record) -> record.getTimestamp() != -1);
  }

  private void exposeOutputStream(final KStream<String, AggregatedActivePowerRecord> aggregations) {
    aggregations.mapValues((final AggregatedActivePowerRecord aggrKieker) -> {
      final Builder aggrAvroBuilder =
          titan.ccp.model.records.AggregatedActivePowerRecord.newBuilder();
      final titan.ccp.model.records.AggregatedActivePowerRecord aggrAvro =
          aggrAvroBuilder.setIdentifier(aggrKieker.getIdentifier())
              .setTimestamp(aggrKieker.getTimestamp()).setMinInW(aggrKieker.getMinInW())
              .setMaxInW(aggrKieker.getMaxInW()).setCount(aggrKieker.getCount())
              .setSumInW(aggrKieker.getSumInW()).setAverageInW(aggrKieker.getAverageInW()).build();
      return aggrAvro;
    }).to(this.outputTopic,
        Produced.with(this.serdes.string(), this.serdes.aggregatedActivePowerRecordValues()));
  }
}
