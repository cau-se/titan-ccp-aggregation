package titan.ccp.aggregation.streamprocessing;

import org.apache.kafka.common.serialization.Serde;
import titan.ccp.avro.SchemaRegistryAvroSerdeFactory;
import titan.ccp.model.records.AggregatedActivePowerRecordAvro;

public class Serdes {

  private final SchemaRegistryAvroSerdeFactory avroSerdeFactory;

  public Serdes(final String schemaRegistryUrl) {
    this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  }

  public Serde<String> string() {
    return org.apache.kafka.common.serialization.Serdes.String();
  }

  public Serde<AggregatedActivePowerRecordAvro> aggregatedActivePowerRecordAvroValues() {
    return this.avroSerdeFactory.forKeys();
  }
}
