package org.apache.beam.issues.io;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UnboundedBoundedJoin {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedBoundedJoin.class);
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
//            .apply(
//                Window.<String>into(new GlobalWindows())
//                    .triggering(
//                        AfterWatermark.pastEndOfWindow()
//                            .withEarlyFirings(
//                                AfterProcessingTime.pastFirstElementInPane()
//                                    .plusDelayOf(Duration.standardSeconds(WINDOW_LENGTH))))
//                    .withAllowedLateness(Duration.standardSeconds(WINDOW_LENGTH))
//                    .accumulatingFiredPanes())
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
                    }))
            .apply(
                "Timestamps",
                WithTimestamps.of(
                    new SerializableFunction<KV<String, String>, Instant>() {

                      @Override
                      public Instant apply(KV<String, String> hv) {
                        return instant.plus(Duration.standardSeconds(30));
//                        return instant;
                      }
                    }))
            .apply(
                "Fixed Windows",
                Window.<KV<String, String>>into(
                        FixedWindows.of(Duration.standardSeconds(WINDOW_LENGTH)))
                    .triggering(
                        AfterWatermark.pastEndOfWindow()
                            .withEarlyFirings(
                                AfterProcessingTime.pastFirstElementInPane()
                                    .plusDelayOf(Duration.standardSeconds(WINDOW_LENGTH))))
                    .withAllowedLateness(Duration.standardSeconds(WINDOW_LENGTH))
                    .accumulatingFiredPanes());

    // CoGroupByKey
    final TupleTag<String> streamTag = new TupleTag<>();
    final TupleTag<String> boundedTag = new TupleTag<>();

    // Perform Co-Group using the key
    final PCollection<KV<String, CoGbkResult>> results = KeyedPCollectionTuple
        .of(streamTag, windowedStream)
        .and(boundedTag, bounded)
        .apply(CoGroupByKey.create());

    PCollection<String> joined =
        results.apply(
            ParDo.of(
                new DoFn<KV<String, CoGbkResult>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    KV<String, CoGbkResult> e = c.element();
                    String key = e.getKey();
                    Iterable<String> streamIter = e.getValue().getAll(streamTag);
                    Iterable<String> boundedIter = e.getValue().getAll(boundedTag);

                    String streamValues = "";
                    String boundedValues = "";

//                    System.out.println("Key: " + key);

                    for (final String s : streamIter) {
                      streamValues = streamValues + "[" + s + "],";
                    }

                    for (final String s : boundedIter) {
                      boundedValues = boundedValues + "[" + s + "],";
                    }

//                    System.out.println("Stream values:" + streamValues);
//                    System.out.println("Bounded values:" + boundedValues);

//                    System.out.println("------------------");

//                    String formattedResult =
//                        Snippets.formatCoGbkResults(name, emailsIter, phonesIter);
//                    c.output(formattedResult);
                    c.output("K: [" + key + "], SVs: " + streamValues + ", BVs: " + boundedValues);
                  }
                }));

    joined.apply(
        ParDo.of(
            new DoFn<String, String>() {
              @ProcessElement
              public void processElement(ProcessContext c) {
                System.out.println(c.element());
              }
            }));

//    dataUnbounded.apply(
//    dataBounded.apply(
//        "WriteToKafka",
//        KafkaIO.<String, String>write()
//            .withBootstrapServers(options.getKafkaServer())
//            .withTopic(options.getOutputTopic())
//            .withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
//            .withValueSerializer(org.apache.kafka.common.serialization.StringSerializer.class));

    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
  }

}
