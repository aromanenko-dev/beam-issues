import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p21p0.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDemo {
  static final Logger LOG = LoggerFactory.getLogger(KafkaDemo.class);

  public interface KafkaDemoOptions
      extends PipelineOptions, StreamingOptions {

    @Description("Kafka bootstrap servers")
    @Default.String("")
    String getBootstrap();
    void setBootstrap(String value);

    @Description("Kafka group.id")
    @Default.String("test.group")
    String getGroupId();
    void setGroupId(String value);

    @Description("Kafka input topic for test")
    @Default.String("test_sample_data")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Kafka output topic for test")
    @Default.String("test_output_data")
    String getOutputTopic();
    void setOutputTopic(String value);
  }

  public static void main(String[] args) {
    KafkaDemoOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(KafkaDemoOptions.class);
    options.setStreaming(true);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> strings = pipeline
        .apply(KafkaIO.<Long, String>read()
            .withBootstrapServers(options.getBootstrap())
            .withTopic(options
                .getInputTopic())  // use withTopics(List<String>) to read from multiple topics.
            .withKeyDeserializer(LongDeserializer.class)
            .withValueDeserializer(StringDeserializer.class)

            // Above four are required configuration. returns PCollection<KafkaRecord<Long, String>>

            // Rest of the settings are optional :

            // you can further customize KafkaConsumer used to read the records by adding more
            // settings for ConsumerConfig. e.g :
            .withConsumerConfigUpdates(ImmutableMap.of("group.id", options.getGroupId()))

            // set event times and watermark based on 'LogAppendTime'. To provide a custom
            // policy see withTimestampPolicyFactory(). withProcessingTime() is the default.
            // Use withCreateTime() with topics that have 'CreateTime' timestamps.
            //            .withLogAppendTime()

            // restrict reader to committed messages on Kafka (see method documentation).
            .withReadCommitted()

            // offset consumed by the pipeline can be committed back.
            .commitOffsetsInFinalize()

            // finally, if you don't need Kafka metadata, you can drop it.g
            .withoutMetadata() // PCollection<KV<Long, String>>
        )
        .apply(Values.create());// PCollection<String>

    strings.apply(KafkaIO.<Void, String>write()
        .withBootstrapServers(options.getBootstrap())
        .withTopic(options.getOutputTopic())
        .withValueSerializer(StringSerializer.class) // just need serializer for value
        .values()
    );

    System.out.println("input topic = " + options.getInputTopic() + ", output topic: " + options.getOutputTopic());
    LOG.info("input topic = " + options.getInputTopic() + ", output topic: " + options.getOutputTopic());

    pipeline.run().waitUntilFinish();
  }

}
