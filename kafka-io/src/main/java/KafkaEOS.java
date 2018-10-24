import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaEOS {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaEOS.class);
  /**
   * Specific pipeline options.
   */
  private interface Options extends PipelineOptions {
    @Description("Kafka Bootstrap Servers")
    @Default.String("localhost:9092")
    String getKafkaServer();
    void setKafkaServer(String value);

    @Description("Kafka Topic Name")
    @Default.String("input")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Kafka Output Topic Name")
    @Default.String("output")
    String getOutputTopic();
    void setOutputTopic(String value);

    @Description("Kafka Sink GroupId")
    @Default.String("myWriterSinkGroupId")
    String getSinkGroupId();
    void setSinkGroupId(String value);
  }

  static int counter = 0;

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    LOG.info(options.toString());
    System.out.println(options.toString());
    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, String>> data =
        pipeline.apply(
            "ReadFromKafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata());

    data.apply(
        "WriteToKafka",
        KafkaIO.<String, String>write()
            .withBootstrapServers(options.getKafkaServer())
            .withTopic(options.getOutputTopic())
            .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
            .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class)
            .withEOS(1, options.getSinkGroupId())
    );

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
  }
}
