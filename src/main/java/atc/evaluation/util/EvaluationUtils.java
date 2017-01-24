package atc.evaluation.util;

import atc.evaluation.TestRunner;

/**
 * Created by taegeonum on 4/29/16.
 */
public class EvaluationUtils {

  public static String[] getCommand(
      final TestRunner.OperatorType operatorType,
      final double inputRate,
      final String outputPath,
      final String timescales,
      final int numThreads,
      final String variable) {
    /*
    return
        "java -Xms22000m -Xmx56000m -cp ::./tempest-0.11-SNAPSHOT.jar vldb.evaluation.WikiWordCountEvaluation "  +
            "--operator_type=" + operatorType.name() +
            "--input_rate=" + inputRate +
            "--output_path=" + outputPath +
            "--timescales=" + timescales +
            "--num_threads=" + numThreads +
            "--variable=" + variable; */
    return
            new String[] {
                "--operator_type=" + operatorType.name(),
                "--input_rate=" + inputRate,
                "--output_path=" + outputPath,
                "--timescales=" + timescales,
                "--num_threads=" + numThreads,
                "--variable=" + variable
            };
  }
}
