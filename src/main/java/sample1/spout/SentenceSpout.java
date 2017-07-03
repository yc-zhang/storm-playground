package sample1.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import utils.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private List sentences = Arrays.asList(
        "my dog has fleas",
        "i like cold beverages",
        "the dog ate my homework",
        "don't have a cow man",
        "I don't think i like fleas"
    );

    private int index = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        this.collector.emit(new Values(sentences.get(index)));
        this.index++;
        if (index >= sentences.size()) {
            this.index = 0;
        }
        Utils.waitForMillis(1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
