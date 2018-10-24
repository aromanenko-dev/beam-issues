package org.apache.beam.issues.io.hadoop;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;

public class HadoopFormatRead {

  private static SerializableConfiguration loadTestConfiguration(
      Class<?> inputFormatClassName, Class<?> keyClass, Class<?> valueClass) {
    Configuration conf = new Configuration();
    conf.setClass("mapreduce.job.inputformat.class", inputFormatClassName, InputFormat.class);
    conf.setClass("key.class", keyClass, Object.class);
    conf.setClass("value.class", valueClass, Object.class);
    return new SerializableConfiguration(conf);
  }

  public static void main(String[] args) {
    SerializableConfiguration serConf =
        loadTestConfiguration(EmployeeInputFormat.class, Text.class, Employee.class);

    Pipeline pipeline = Pipeline.create();

    HadoopFormatIO.Read<Text, Employee> read =
        HadoopFormatIO.<Text, Employee>read().withConfiguration(serConf.get());

    pipeline
        .apply("ReadTest", read)
        .apply(
            ParDo.of(
                new DoFn<KV<Text, Employee>, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctx) {
                    System.out.println(
                        "key: " + ctx.element().getKey() + ", val: " + ctx.element().getValue());
                  }
                }));
    pipeline.run();
  }
}
