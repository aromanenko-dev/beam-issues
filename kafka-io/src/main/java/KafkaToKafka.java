import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.ProducerRecordCoder;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaToKafka {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToKafka.class);
  /**
   * Specific pipeline options.
   */
  public interface Options extends PipelineOptions {
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

    @Description("Pipeline run mode [pr, kv]")
    @Default.String("pr")
    String getRunMode();
    void setRunMode(String value);
  }

  static int counter = 0;

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    LOG.info(options.toString());
    System.out.println(options.toString());
    Pipeline pipeline = Pipeline.create(options);

    if (options.getRunMode().equals("pr")) {
      runWithProducerRecords(options, pipeline);
    } else if (options.getRunMode().equals("kv")) {
      runWithKV(options, pipeline);
    } else {
      throw new RuntimeException("Unknown run mode: " + options.getRunMode());
    }

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
  }

  private static void runWithProducerRecords(Options options, Pipeline pipeline) {
    PCollection<ProducerRecord<String, String>> data =
        pipeline
            .apply(
                "ReadFromKafka",
                KafkaIO.<String, String>read()
                    .withBootstrapServers(options.getKafkaServer())
                    .withTopic(options.getInputTopic())
                    .withKeyDeserializer(StringDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)
//                    .withMaxNumRecords(5)
                    .withMaxReadTime(Duration.standardSeconds(10))
                    .withoutMetadata())
            .apply(ParDo.of(new KV2ProducerRecord(options.getInputTopic())))
            .setCoder(ProducerRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    data.apply(
        "WriteToKafka",
        KafkaIO.<String, String>writeRecords()
            .withBootstrapServers(options.getKafkaServer())
            .withTopic(options.getOutputTopic())
            .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
            .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class));
  }

  private static void runWithKV(Options options, Pipeline pipeline) {
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
            .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class));
  }

  private static class KV2ProducerRecord
      extends DoFn<KV<String, String>, ProducerRecord<String, String>> {
    final String topic;

    KV2ProducerRecord(String topic) {
      this.topic = topic;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      KV<String, String> kv = ctx.element();
      String key = kv.getKey() == null ? "null" : kv.getKey();
      String value = kv.getValue() == null ? "null" : kv.getValue();

      LOG.info("counter: " + counter);
      System.out.println("counter = " + counter);

      if (counter++ % 2 == 0) {
        ctx.output(new ProducerRecord<>("topic2", key, value));
      } else {
        ctx.output(new ProducerRecord<>("topic1", key, value));
      }
    }
  }
}
