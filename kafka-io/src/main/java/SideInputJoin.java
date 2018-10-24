import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SideInputJoin {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputJoin.class);
  public static final int WINDOW_LENGTH = 20;

  private interface Options extends PipelineOptions {
    @Description("Kafka Bootstrap Servers")
    @Default.String("localhost:9092")
    String getKafkaServer();
    void setKafkaServer(String value);

    @Description("Kafka Input Topic Name")
    @Default.String("input")
    String getInputTopic();
    void setInputTopic(String value);

    @Description("Kafka Output Topic Name")
    @Default.String("output")
    String getOutputTopic();
    void setOutputTopic(String value);

    @Description("Input File Path")
    @Default.String("/tmp/input.csv")
    String getInputFile();
    void setInputFile(String value);
  }

  public static void main(String[] args) throws Exception {

    Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    LOG.info(options.toString());
    Pipeline pipeline = Pipeline.create(options);

    // Kafka stream
    final PCollection<KV<String, String>> stream =
        pipeline.apply(
            "ReadFromKafka",
            KafkaIO.<String, String>read()
                .withBootstrapServers(options.getKafkaServer())
                .withTopic(options.getInputTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata());

    final PCollection<KV<String, String>> windowedStream =
        stream
            .apply(
                Window.<KV<String, String>>into(FixedWindows.of(Duration.standardSeconds(
                    WINDOW_LENGTH)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardSeconds(WINDOW_LENGTH))))
                    .withAllowedLateness(Duration.standardSeconds(WINDOW_LENGTH))
                    .accumulatingFiredPanes())
            .apply(
                ParDo.of(
                    new DoFn<KV<String, String>, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext processContext) {
                        String value = processContext.element().getValue();
                        String[] split = value.split(",");
                        processContext.output(KV.of(split[0], split[1]));
                      }
                    }));

    // File bounded source
    final Instant instant = new Instant().toDateTime(DateTimeZone.UTC).toInstant();

    PCollection<KV<String, String>> bounded =
        pipeline
            .apply(TextIO.read().from(options.getInputFile()))
            .apply(
                ParDo.of(
                    new DoFn<String, KV<String, String>>() {
                      @ProcessElement
                      public void processElement(ProcessContext processContext) {
                        String element = processContext.element();
                        System.out.println("TS: " + processContext.timestamp() + ", element: " + element);
                        String[] split = element.split(",");
                        processContext.output(KV.of(split[0], split[1]));
                      }
                    }));

    PCollectionView<Map<String, String>> boundedMap = bounded.apply(View.asMap());


    PCollection<String> joined =
        windowedStream.apply(
            ParDo.of(
                new DoFn<KV<String, String>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, String> e = c.element();
                    String key = e.getKey();

                    String sideInputData = c.sideInput(boundedMap).get(key);
                    String streamValues = e.getValue();
                    String boundedValues = sideInputData != null ? sideInputData : "null";

                    c.output("K: [" + key + "], SVs: " + streamValues + ", BVs: " + boundedValues);
                  }
                }).withSideInputs(boundedMap));

    joined.apply(
        ParDo.of(
            new DoFn<String, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                System.out.println(c.element());
              }
            }));

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
  }

}
