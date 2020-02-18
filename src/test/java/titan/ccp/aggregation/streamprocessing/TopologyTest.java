package titan.ccp.aggregation.streamprocessing;


import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import titan.ccp.configuration.events.Event;
import titan.ccp.configuration.events.EventSerde;
import titan.ccp.model.records.ActivePowerRecord;
import titan.ccp.model.records.AggregatedActivePowerRecord;
import titan.ccp.model.sensorregistry.MutableAggregatedSensor;
import titan.ccp.model.sensorregistry.MutableSensorRegistry;

public class TopologyTest {

  private static final String INPUT_TOPIC = "input";
  private static final String OUTPUT_TOPIC = "output";
  private static final String FEEDBACK_TOPIC = "feedback";
  private static final String CONFIGURATION_TOPIC = "configuration";

  private TopologyTestDriver testDriver;
  private TestInputTopic<Event, String> configurationTopic;
  private TestInputTopic<String, ActivePowerRecord> inputTopic;
  private TestOutputTopic<String, AggregatedActivePowerRecord> outputTopic;
  // private TestOutputTopic<String, AggregatedActivePowerRecord> feedbackTopic;
  private Serdes serdes;

  @Before
  public void setup() {
    this.serdes = new MockedSchemaRegistrySerdes();

    final Topology topology = new TopologyBuilder(
        this.serdes,
        INPUT_TOPIC,
        OUTPUT_TOPIC,
        CONFIGURATION_TOPIC,
        Duration.ofSeconds(3),
        Duration.ofSeconds(10)).build();
    // inal Serdes serdes, final String inputTopic, final String outputTopic,
    // final String configurationTopic, final Duration windowSize, final Duration gracePeriod

    // setup test driver
    final Properties props = new Properties();
    props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "dummy-aggregation");
    props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    // props.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "0"); // TODO requird?
    // props.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0"); // TODO requird?
    this.testDriver = new TopologyTestDriver(topology, props);
    this.inputTopic = this.testDriver.createInputTopic(
        INPUT_TOPIC,
        this.serdes.string().serializer(),
        this.serdes.activePowerRecordValues().serializer());
    this.configurationTopic = this.testDriver.createInputTopic(
        CONFIGURATION_TOPIC,
        EventSerde.serde().serializer(),
        this.serdes.string().serializer());
    this.testDriver.createInputTopic(
        OUTPUT_TOPIC,
        this.serdes.string().serializer(),
        this.serdes.aggregatedActivePowerRecordValues().serializer());
    this.testDriver.createInputTopic(
        FEEDBACK_TOPIC,
        this.serdes.string().serializer(),
        this.serdes.aggregatedActivePowerRecordValues().serializer());
    this.outputTopic = this.testDriver.createOutputTopic(
        OUTPUT_TOPIC,
        this.serdes.string().deserializer(),
        this.serdes.aggregatedActivePowerRecordValues().deserializer());
    // this.feedbackTopic = this.testDriver.createOutputTopic(
    // FEEDBACK_TOPIC,
    // this.serdes.string().deserializer(),
    // this.serdes.aggregatedActivePowerRecordValues().deserializer());

  }

  @After
  public void tearDown() {
    this.testDriver.close();
  }

  @Test
  public void testOneLevelRegistry() {
    // Publish sensor registry
    final MutableSensorRegistry registry = new MutableSensorRegistry("root");
    final MutableAggregatedSensor root = registry.getTopLevelSensor();
    root.addChildMachineSensor("child1");
    root.addChildMachineSensor("child2");
    this.configurationTopic.pipeInput(
        Event.SENSOR_REGISTRY_CHANGED,
        registry.toJson(),
        Instant.ofEpochSecond(0));

    // Publish input records
    this.pipeInput("child1", Instant.ofEpochSecond(1), 100.0);
    this.pipeInput("child2", Instant.ofEpochSecond(2), 100.0);

    // Advance time to obtain outputs
    // this.testDriver.advanceWallClockTime(Duration.ofSeconds(10));
    this.pipeInput("child2", Instant.ofEpochSecond(4), 123.0);

    // Check aggregation results
    Assert.assertEquals(1, this.outputTopic.getQueueSize());

    final AggregatedActivePowerRecord result = this.outputTopic.readValue();
    Assert.assertEquals("root", result.getIdentifier());
    Assert.assertEquals(2, result.getCount());
    Assert.assertEquals(200.0, result.getSumInW(), 0.1);

  }

  @Test
  public void testTwoLevelRegistry() {
    // Publish sensor registry
    final MutableSensorRegistry registry = new MutableSensorRegistry("root");
    final MutableAggregatedSensor root = registry.getTopLevelSensor();
    final MutableAggregatedSensor sensor1 = root.addChildAggregatedSensor("sensor-1");
    sensor1.addChildAggregatedSensor("sensor-1-1");
    sensor1.addChildAggregatedSensor("sensor-1-2");
    final MutableAggregatedSensor sensor2 = root.addChildAggregatedSensor("sensor-2");
    sensor2.addChildAggregatedSensor("sensor-2-1");
    sensor2.addChildAggregatedSensor("sensor-2-2");
    this.configurationTopic.pipeInput(
        Event.SENSOR_REGISTRY_CHANGED,
        registry.toJson(),
        Instant.ofEpochSecond(0));

    // Publish input records
    this.pipeInput("sensor-1-1", Instant.ofEpochSecond(1), 100.0);
    this.pipeInput("sensor-2-1", Instant.ofEpochSecond(1), 100.0);
    this.pipeInput("sensor-1-2", Instant.ofEpochSecond(2), 100.0);
    this.pipeInput("sensor-2-2", Instant.ofEpochSecond(2), 100.0);

    // Advance time to obtain outputs
    // this.testDriver.advanceWallClockTime(Duration.ofSeconds(10)); // Does not work
    this.pipeInput("sensor-2-2", Instant.ofEpochSecond(4), 123.0);

    // Check aggregation results
    Assert.assertEquals(3, this.outputTopic.getQueueSize());

    final AggregatedActivePowerRecord rootResult = this.outputTopic.readValue();
    Assert.assertEquals("root", rootResult.getIdentifier());
    Assert.assertEquals(2, rootResult.getCount());
    Assert.assertEquals(400.0, rootResult.getSumInW(), 0.1);

    final AggregatedActivePowerRecord sensor1Result = this.outputTopic.readValue();
    Assert.assertEquals("sensor-1", sensor1Result.getIdentifier());
    Assert.assertEquals(2, sensor1Result.getCount());
    Assert.assertEquals(200.0, sensor1Result.getSumInW(), 0.1);

    final AggregatedActivePowerRecord sensor2Result = this.outputTopic.readValue();
    Assert.assertEquals("sensor-2", sensor2Result.getIdentifier());
    Assert.assertEquals(2, sensor2Result.getCount());
    Assert.assertEquals(200.0, sensor2Result.getSumInW(), 0.1);
  }

  private void pipeInput(final String identifier, final Instant timestamp, final double value) {
    this.inputTopic.pipeInput(
        identifier,
        new ActivePowerRecord(identifier, timestamp.toEpochMilli(), value),
        timestamp);
  }

}
