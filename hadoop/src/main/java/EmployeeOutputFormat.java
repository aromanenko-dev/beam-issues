package org.apache.beam.issues.io.hadoop;

import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmployeeOutputFormat extends OutputFormat<Text, Employee> {
  private static volatile List<KV<Text, Employee>> output;
  private static OutputCommitter outputCommitter;

  @Override
  public RecordWriter<Text, Employee> getRecordWriter(TaskAttemptContext context) {
    return new RecordWriter<Text, Employee>() {
      @Override
      public void write(Text key, Employee value) {
        KV<Text, Employee> kv = KV.of(key, value);
        System.out.println("kv = " + kv);
        output.add(kv);
      }

      @Override
      public void close(TaskAttemptContext context) {}
    };
  }

  @Override
  public void checkOutputSpecs(JobContext context) {}

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return outputCommitter;
  }

  static synchronized void initWrittenOutput(OutputCommitter outputCommitter) {
    EmployeeOutputFormat.outputCommitter = outputCommitter;
    output = Collections.synchronizedList(new ArrayList<>());
  }

  static List<KV<Text, Employee>> getWrittenOutput() {
    return output;
  }

  static OutputCommitter getOutputCommitter() {
    return outputCommitter;
  }
}
