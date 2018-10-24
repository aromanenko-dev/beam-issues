import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.LongDeserializer;

public class KafkaAvro {

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);

    KafkaIO.Read read = KafkaIO.<Long, MyClass>read().withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializerAndCoder((Class) KafkaAvroDeserializer.class,
            AvroCoder.of(MyClass.class));
    p.apply(read);

    p.run();
  }

  @DefaultCoder(AvroCoder.class)
  public class MyClass {

    String name;
    String age;

    MyClass() {}

    MyClass(String n, String a) {
      this.name = n;
      this.age = a;
    }
  }

}
