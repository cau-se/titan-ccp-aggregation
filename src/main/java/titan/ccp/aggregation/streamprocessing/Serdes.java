package titan.ccp.aggregation.streamprocessing;

import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.avro.SchemaRegistryAvroSerdeFactory;
import titan.ccp.model.records.AggregatedActivePowerRecord;

public class Serdes {

  private final SchemaRegistryAvroSerdeFactory avroSerdeFactory;

  public Serdes(final String schemaRegistryUrl) {
    this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  }

  public Serde<String> string() {
    return org.apache.kafka.common.serialization.Serdes.String();
  }

  public Serde<AggregatedActivePowerRecord> aggregatedActivePowerRecordAvroValues() {
    return this.avroSerdeFactory.forKeys();
  }
}
