package org.apache.beam.issues.io.hadoop;

import com.google.common.base.Splitter;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TestEmployeeDataSet {
  public static final long NUMBER_OF_RECORDS_IN_EACH_SPLIT = 5L;
  public static final long NUMBER_OF_SPLITS = 3L;
  private static final List<KV<String, String>> data = new ArrayList<>();

  public synchronized static List<KV<String, String>> populateEmployeeData() {
    if (!data.isEmpty()) {
      return data;
    }
    data.add(KV.of("0", "Alex_US"));
    data.add(KV.of("1", "John_UK"));
    data.add(KV.of("2", "Tom_UK"));
    data.add(KV.of("3", "Nick_UAE"));
    data.add(KV.of("4", "Smith_IND"));
    data.add(KV.of("5", "Taylor_US"));
    data.add(KV.of("6", "Gray_UK"));
    data.add(KV.of("7", "James_UAE"));
    data.add(KV.of("8", "Jordan_IND"));
    data.add(KV.of("9", "Leena_UK"));
    data.add(KV.of("10", "Zara_UAE"));
    data.add(KV.of("11", "Talia_IND"));
    data.add(KV.of("12", "Rose_UK"));
    data.add(KV.of("13", "Kelvin_UAE"));
    data.add(KV.of("14", "Goerge_IND"));
    return data;
  }

  public static List<KV<Text, Employee>> getEmployeeData() {
    return (data.isEmpty() ? populateEmployeeData() : data)
        .stream()
        .map(
            input -> {
              List<String> empData = Splitter.on('_').splitToList(input.getValue());
              return KV.of(
                  new Text(input.getKey()), new Employee(empData.get(0), empData.get(1)));
            })
        .collect(Collectors.toList());
  }
}
