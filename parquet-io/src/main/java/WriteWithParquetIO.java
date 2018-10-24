import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteWithParquetIO {

  private static final Logger LOG = LoggerFactory.getLogger(WriteWithParquetIO.class);

  private static final Schema SCHEMA = new Schema.Parser().parse("{\n"
      + " \"namespace\": \"ioitavro\",\n"
      + " \"type\": \"record\",\n"
      + " \"name\": \"TestAvroLine\",\n"
      + " \"fields\": [\n"
      + "     {\"name\": \"row\", \"type\": \"string\"}\n"
      + " ]\n"
      + "}");

  /**
   * Constructs text lines in files used for testing.
   */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      System.out.println("c.element() = " + c.element());
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  private static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(
          new GenericRecordBuilder(SCHEMA).set("row", c.element()).build()
      );
    }
  }

  public static void main(String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    Pipeline pipeline = Pipeline.create(options);
    pipeline
//        .apply("Generate sequence", GenerateSequence.from(0).to(10))
        .apply("Generate sequence", GenerateSequence.from(0))
        .apply(Window.<Long>into(FixedWindows.of(Duration.standardSeconds(5))))
        .apply("Produce text lines", ParDo.of(new DeterministicallyConstructTestTextLineFn()))
        .apply("Produce Avro records", ParDo.of(new DeterministicallyConstructAvroRecordsFn()))
        .setCoder(AvroCoder.of(SCHEMA)).apply("Write Parquet files",
        FileIO.<GenericRecord>write().via(ParquetIO.sink(SCHEMA)).to(options.getOutput())
            .withNumShards(0));

    pipeline.run();
  }

  /**
   * Specific pipeline options.
   */
  public interface Options extends PipelineOptions {
    @Description("Output Path")
    @Default.String("/tmp/parquet-io")
    String getOutput();
    void setOutput(String value);
  }

}
