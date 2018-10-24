package org.apache.beam.issues.io.aws;

import com.google.auto.service.AutoService;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.FileSystemRegistrar;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.hdfs.HadoopFileSystemOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

public class MultipleS3Credentials {

  public interface Options extends HadoopFileSystemOptions {
//  public interface Options extends PipelineOptions, AwsOptions {
//  public interface Options extends PipelineOptions {

  }

//  @AutoService(FileSystemRegistrar.class)
//  @Experimental(Experimental.Kind.FILESYSTEM)
//  public class S3aFileSystemRegistrar implements FileSystemRegistrar {
//
//    @Override
//    public Iterable<FileSystem> fromOptions(@Nonnull PipelineOptions options) {
////      checkNotNull(options, "Expect the runner have called FileSystems.setDefaultPipelineOptions().");
//      return ImmutableList.of(new S3FileSystem(options.as(S3Options.class)));
//    }
//  }

  public static void main(String[] args) {

    HadoopFileSystemOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(HadoopFileSystemOptions.class);
    System.out.println("options = " + options);


//    Configuration hdfsConfiguration = new Configuration();
//    hdfsConfiguration.set("fs.default.name", "s3://apache-beam-parquet/");
//    hdfsConfiguration.set("fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020/");
//    hdfsConfiguration.set("fs.default.name", "hdfs://sandbox-hdp.hortonworks.com:8020/");
//    hdfsConfiguration.set("fs.s3a.access.key", "");
//    hdfsConfiguration.set("fs.s3a.secret.key", "");


//    List<Configuration> confs = new ArrayList<>();
//    confs.add(hdfsConfiguration);
//    options.setHdfsConfiguration(confs);

    Pipeline pipeline = Pipeline.create(options);




//    options.setAwsCredentialsProvider(
//        new AWSStaticCredentialsProvider(new BasicAWSCredentials("cred1", "cred1Secret")));

//    options.setAwsCredentialsProvider(new AWSCredentialsProviderChain(
//        new AWSStaticCredentialsProvider(new BasicAWSCredentials("cred1", "cred1Secret")),
//        new AWSStaticCredentialsProvider(new BasicAWSCredentials("cred2", "cred2Secret"))));

//    options.setAwsCredentialsProvider(new DefaultAWSCredentialsProviderChain());

//    String inputFileName = "s3://apache-beam-parquet/input.txt";
    String inputFileName = "hdfs://sandbox-hdp.hortonworks.com:8020//tmp/input.txt";
//    String inputFileName = "/tmp/input.txt";
//    String outputFileName = "s3://apache-beam-parquet/output.txt";
    String outputFileName = "hdfs://sandbox-hdp.hortonworks.com:8020//tmp/output.txt";
//    String outputFileName = "/tmp/output.txt";

    PCollection<String> input =
        pipeline
            .apply("ReadInput", TextIO.read().from(inputFileName))
            .apply("Transform", ParDo.of(new DoFn<String, String>() {
              @ProcessElement
              public void processElement(ProcessContext ctx) {
                System.out.println("ctx.element() = " + ctx.element());
              }
            }));

//    options.setAwsCredentialsProvider(
//        new AWSStaticCredentialsProvider(new BasicAWSCredentials("cred2", "cred2Secret")));

    input.apply("WriteOutput", TextIO.write().to(outputFileName));
    pipeline.run().waitUntilFinish();
  }

}
