package evaluation.example.common;

import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.OutputEmitter;

import javax.inject.Inject;

public final class WikiSplitOperator implements Operator<String, String> {

  private OutputEmitter<String> emitter;

  @Inject
  public WikiSplitOperator() {

  }

  @Override
  public void execute(final String val) {
    final String[] split = val.split(" ");
    for (int i = 0; i < split.length; i++) {
      emitter.emit(split[i]);
    }
  }

  @Override
  public void prepare(final OutputEmitter<String> outputEmitter) {
    emitter = outputEmitter;
  }
}
