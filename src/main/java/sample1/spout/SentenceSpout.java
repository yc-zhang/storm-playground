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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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

    private Map<UUID, Values> pending;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<>();
    }

    @Override
    public void nextTuple() {
        Values values = new Values(sentences.get(index));
        this.pending.put(UUID.randomUUID(), values);
        this.collector.emit(values);
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

    public void ack(Object messageId) {
        this.pending.remove(messageId);
    }

    public void fail(Object messageId) {
        this.collector.emit(this.pending.get(messageId), messageId);
    }
}
