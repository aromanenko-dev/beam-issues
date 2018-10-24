package org.apache.beam.issues.io.hadoop;

import com.google.common.base.Splitter;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EmployeeInputFormat extends InputFormat<Text, Employee> {

  public EmployeeInputFormat() {}

  @Override
  public RecordReader<Text, Employee> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new EmployeeRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext arg0) {
    List<InputSplit> inputSplitList = new ArrayList<>();
    for (int i = 1; i <= TestEmployeeDataSet.NUMBER_OF_SPLITS; i++) {
      InputSplit inputSplitObj =
          new NewObjectsEmployeeInputSplit(
              (i - 1) * TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT,
              i * TestEmployeeDataSet.NUMBER_OF_RECORDS_IN_EACH_SPLIT - 1);
      inputSplitList.add(inputSplitObj);
    }
    return inputSplitList;
  }

  /** InputSplit implementation for EmployeeInputFormat. */
  public static class NewObjectsEmployeeInputSplit extends InputSplit implements Writable {
    // Start and end map index of each split of employeeData.
    private long startIndex;
    private long endIndex;

    public NewObjectsEmployeeInputSplit() {}

    public NewObjectsEmployeeInputSplit(long startIndex, long endIndex) {
      this.startIndex = startIndex;
      this.endIndex = endIndex;
    }

    /** Returns number of records in each split. */
    @Override
    public long getLength() {
      return this.endIndex - this.startIndex + 1;
    }

    @Override
    public String[] getLocations() {
      return null;
    }

    long getStartIndex() {
      return startIndex;
    }

    public long getEndIndex() {
      return endIndex;
    }

    @Override
    public void readFields(DataInput dataIn) throws IOException {
      startIndex = dataIn.readLong();
      endIndex = dataIn.readLong();
    }

    @Override
    public void write(DataOutput dataOut) throws IOException {
      dataOut.writeLong(startIndex);
      dataOut.writeLong(endIndex);
    }
  }

  /** RecordReader for EmployeeInputFormat. */
  public static class EmployeeRecordReader extends RecordReader<Text, Employee> {

    private NewObjectsEmployeeInputSplit split;
    private Text currentKey;
    private Employee currentValue;
    private long employeeListIndex = 0L;
    private long recordsRead = 0L;
    private List<KV<String, String>> employeeDataList;

    public EmployeeRecordReader() {}

    @Override
    public void close() {}

    @Override
    public Text getCurrentKey() {
      return currentKey;
    }

    @Override
    public Employee getCurrentValue() {
      return currentValue;
    }

    @Override
    public float getProgress() {
      return (float) recordsRead / split.getLength();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext arg1) throws InterruptedException {
      this.split = (NewObjectsEmployeeInputSplit) split;
      employeeListIndex = this.split.getStartIndex() - 1;
      recordsRead = 0;
      employeeDataList = TestEmployeeDataSet.populateEmployeeData();
      currentValue = new Employee(null, null);

      Thread.sleep(1000);
    }

    @Override
    public boolean nextKeyValue() {
      if ((recordsRead++) >= split.getLength()) {
        return false;
      }
      employeeListIndex++;
      KV<String, String> employeeDetails = employeeDataList.get((int) employeeListIndex);

      String value = employeeDetails.getValue();

      List<String> empData = Splitter.on('_').splitToList(value);
      /*
       * New objects must be returned every time for key and value in order to test the scenario as
       * discussed the in the class' javadoc.
       */
      currentKey = new Text(employeeDetails.getKey());
      currentValue = new Employee(empData.get(0), empData.get(1));
      return true;
    }
  }
}