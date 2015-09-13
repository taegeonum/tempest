package evaluation.example.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.snu.tempest.operator.Operator;
import edu.snu.tempest.operator.OperatorConnector;
import edu.snu.tempest.operator.OutputEmitter;
import edu.snu.tempest.operator.filter.FilterFunction;
import edu.snu.tempest.operator.filter.FilterOperator;
import evaluation.example.common.WikiFilterFunction;
import evaluation.example.common.WikiSplitOperator;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.util.Map;

public class WordCountFilterSplitBolt extends BaseRichBolt {

  private Operator<String, String> operator;

  public WordCountFilterSplitBolt() {

  }

  @Override
  public void prepare(final Map map, final TopologyContext topologyContext, final OutputCollector outputCollector) {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(FilterFunction.class, WikiFilterFunction.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    try {
      operator = injector.getInstance(FilterOperator.class);
      final WikiSplitOperator splitOperator = new WikiSplitOperator();
      operator.prepare(new OperatorConnector<>(splitOperator));
      splitOperator.prepare(new OutputEmitter<String>() {
        @Override
        public void emit(final String output) {
          outputCollector.emit(new Values(output));
        }
      });
    } catch (InjectionException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void execute(final Tuple tuple) {
    operator.execute(tuple.getString(0));
  }

  @Override
  public void declareOutputFields(final OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("word"));
  }
}
