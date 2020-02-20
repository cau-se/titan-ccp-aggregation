package titan.ccp.aggregation.streamprocessing;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;

public class MockedSchemaRegistrySerdes extends Serdes {

  private static final String URL_KEY = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String DUMMY_URL = "http://dummy";

  private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
  private final Map<String, String> serdeConfig = Collections.singletonMap(URL_KEY, DUMMY_URL);

  public MockedSchemaRegistrySerdes() {
    super(DUMMY_URL);
  }

  @Override
  public Serde<ActivePowerRecord> activePowerRecordValues() {
    return this.buildAvroSerde(false);
  }

  @Override
  public Serde<AggregatedActivePowerRecord> aggregatedActivePowerRecordValues() {
    return this.buildAvroSerde(false);
  }

  private <T extends SpecificRecord> Serde<T> buildAvroSerde(final boolean isKey) {
    final Serde<T> avroSerde = new SpecificAvroSerde<>(this.schemaRegistryClient);
    avroSerde.configure(this.serdeConfig, isKey);
    return avroSerde;
  }

}
