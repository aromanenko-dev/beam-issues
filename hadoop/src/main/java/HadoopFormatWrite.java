package org.apache.beam.issues.io.hadoop;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.format.HDFSSynchronization;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class HadoopFormatWrite {

  private static TemporaryFolder tmpFolder;
  private static final String LOCKS_FOLDER_NAME = "locks";

  private static Configuration loadTestConfiguration(
      Class<?> outputFormatClassName, Class<?> keyClass, Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR, outputFormatClassName, OutputFormat.class);
    conf.setClass(MRJobConfig.OUTPUT_KEY_CLASS, keyClass, Object.class);
    conf.setClass(MRJobConfig.OUTPUT_VALUE_CLASS, valueClass, Object.class);
    conf.set(MRJobConfig.ID, String.valueOf(1));
    return conf;
  }

  private static String getLocksDirPath() {
    return Paths.get(tmpFolder.getRoot().getAbsolutePath(), LOCKS_FOLDER_NAME)
        .toAbsolutePath()
        .toString();
  }

  public static void main(String[] args) throws IOException {
    tmpFolder = new TemporaryFolder(new File("/tmp"));
    tmpFolder.create();

    Configuration conf =
        loadTestConfiguration(EmployeeOutputFormat.class, Text.class, Employee.class);
    conf.set(HadoopFormatIO.OUTPUT_DIR, tmpFolder.getRoot().getAbsolutePath());

    OutputCommitter mockedOutputCommitter = Mockito.mock(OutputCommitter.class);
    EmployeeOutputFormat.initWrittenOutput(mockedOutputCommitter);

    SerializableConfiguration hadoopConfiguration = new SerializableConfiguration(conf);

    Pipeline pipeline = Pipeline.create();

    List<KV<Text, Employee>> data = TestEmployeeDataSet.getEmployeeData();
    PCollection<KV<Text, Employee>> input =
        pipeline
            .apply(Create.of(data))
            .setTypeDescriptor(
                TypeDescriptors.kvs(
                    new TypeDescriptor<Text>() {}, new TypeDescriptor<Employee>() {}));

    input.apply(
        "Write",
        HadoopFormatIO.<Text, Employee>write()
            .withConfiguration(hadoopConfiguration.get())
            .withPartitioning()
            .withExternalSynchronization(new HDFSSynchronization(getLocksDirPath()))
    );
    pipeline.run().waitUntilFinish();
  }
}
