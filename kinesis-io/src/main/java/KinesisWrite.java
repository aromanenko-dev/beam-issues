import com.amazonaws.regions.Regions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kinesis.KinesisIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

public class KinesisWrite {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisWrite.class);

  public static final int NUM_RECORDS = 10;

  /**
   * Specific pipeline options.
   */
  public interface Options extends PipelineOptions {
    String getAwsKinesisStream();
    void setAwsKinesisStream(String value);

    String getAwsAccessKey();
    void setAwsAccessKey(String value);

    String getAwsSecretKey();
    void setAwsSecretKey(String value);

    String getAwsKinesisRegion();
    void setAwsKinesisRegion(String value);
  }

  public static void main(String[] args) throws Exception {
    Options options = PipelineOptionsFactory
        .fromArgs(args).withValidation().as(Options.class);

    LOG.info(options.toString());
    System.out.println(options.toString());
    Pipeline pipeline = Pipeline.create(options);

    List<byte[]> inputData = prepareData();

    // Write data into stream
    pipeline.apply(Create.of(inputData))
        .apply(
            KinesisIO.write()
                .withStreamName(options.getAwsKinesisStream())
                .withPartitionKey("key")
                .withAWSClientsProvider(
                    options.getAwsAccessKey(),
                    options.getAwsSecretKey(),
                    Regions.fromName(options.getAwsKinesisRegion())));
    pipeline.run().waitUntilFinish();
  }

  private static List<byte[]> prepareData() {
    List<byte[]> data = newArrayList();
    for (int i = 0; i < NUM_RECORDS; i++) {
      data.add(String.valueOf(i).getBytes(StandardCharsets.UTF_8));
    }
    return data;
  }

}
