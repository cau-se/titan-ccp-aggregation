package titan.ccp.aggregation.streamprocessing;

import org.apache.kafka.common.serialization.Serde;
import titan.ccp.common.avro.SchemaRegistryAvroSerdeFactory;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.ActivePowerRecordKey;
import titan.ccp.model.records.AggregatedActivePowerRecord;

public class Serdes {

  private final SchemaRegistryAvroSerdeFactory avroSerdeFactory;

  public Serdes(final String schemaRegistryUrl) {
    this.avroSerdeFactory = new SchemaRegistryAvroSerdeFactory(schemaRegistryUrl);
  }

  public Serde<String> string() {
    return org.apache.kafka.common.serialization.Serdes.String();
  }

  public Serde<ActivePowerRecordKey> activePowerRecordKeys() {
    return this.avroSerdeFactory.forKeys();
  }

  public Serde<ActivePowerRecord> activePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }

  public Serde<AggregatedActivePowerRecord> aggregatedActivePowerRecordValues() {
    return this.avroSerdeFactory.forValues();
  }
}
