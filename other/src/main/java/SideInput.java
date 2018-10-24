import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.List;

public class SideInput {

  private interface Options extends PipelineOptions {
    @Default.String("UserValue")
    String getUserValue();
    void setUserValue(String value);
  }

  public static void main(String[] args) {
    // Create some sample data to be fed to our c++ Echo library
    List<String> sampleData = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      sampleData.add("word" + String.valueOf(i));
    }

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);

    // The input PCollection to ParDo.
    PCollection<String> words = p.apply(Create.of(sampleData));

    // A PCollection of word lengths that we'll combine into a single value.
    //    PCollection<Integer> wordLengths; // Singleton PCollection

    // Create a singleton PCollectionView from wordLengths using Combine.globally and
    // View.asSingleton.
    //    final PCollectionView<Integer> maxWordLengthCutOffView =
    //        wordLengths.apply(Combine.globally(new Max.MaxIntFn()).asSingletonView());


    String userValue = options.getUserValue();

    PCollection<String> wordsBelowCutOff =
        words.apply(
            ParDo.of(
                new DoFn<String, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    PipelineOptions opt = c.getPipelineOptions();
                    System.out.println(userValue + "_" + c.element());
                  }
                }));

    p.run().waitUntilFinish();

//        words.apply(ParDo
//            .of(new DoFn<String, String>() {
//              @ProcessElement
//              public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext c) {
//                // In our DoFn, access the side input.
//                int lengthCutOff = c.sideInput(maxWordLengthCutOffView);
//                if (word.length() <= lengthCutOff) {
//                  out.output(word);
//                }
//              }
//            }).withSideInputs(maxWordLengthCutOffView)
//        );

  }
}
