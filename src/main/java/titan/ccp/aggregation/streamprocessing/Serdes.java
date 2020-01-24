package titan.ccp.aggregation.streamprocessing;

import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.avro.SchemaRegistryAvroSerdeFactory;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;

/**
 * Class that contains all the needed serializer/deserializer.
 *
 */
public class Serdes {

  private final SchemaRegistryAvroSerdeFactory avroSerdeFactory;

  public Serdes(final String schemaRegistryUrl) {
    this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  }

  public Serde<String> string() {
    return org.apache.kafka.common.serialization.Serdes.String();
  }

  public Serde<ActivePowerRecord> activePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }

  public Serde<AggregatedActivePowerRecord> aggregatedActivePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }
}
