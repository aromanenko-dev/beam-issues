import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.*;
import java.nio.charset.Charset;

@RunWith(JUnit4.class)
public class CoderTests implements Serializable {

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testCoder() throws Exception {
    PCollection<String> input = p.apply(Create.of("aaa", "bbb", "ccc"));
    PCollection<MyData> output = input.apply(ParDo.of(new RawEventToMyDataFn()));
    output.setCoder(new TestCoder());
    p.run();
  }

  private class RawEventToMyDataFn extends DoFn<String, MyData> {

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      ctx.output(new MyData(ctx.element()));
    }

  }

  public static class TestCoder extends CustomCoder<MyData> {
    @Override
    public void encode(MyData value, OutputStream outStream)
        throws CoderException, IOException {
      byte[] bytes = value.strDataProp.getBytes(Charset.defaultCharset());
      outStream.write(bytes);
    }

    @Override
    public MyData decode(InputStream inStream) throws CoderException, IOException {
      try {
        InputStreamReader reader = new InputStreamReader(inStream);
        StringBuilder out = new StringBuilder();
        int c;
        while ((c = reader.read()) != -1) {
          out.append((char) c);
        }
        System.out.println(out.toString());
        return new MyData(out.toString());
      } catch (CoderException e) {
        e.printStackTrace();
        return null;
      }
    }
  }

  public static class MyData {

    String strDataProp;

    public MyData(String toString) {
      strDataProp = toString;
    }
  }
}
