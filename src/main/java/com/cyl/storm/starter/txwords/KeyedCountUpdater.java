package com.cyl.storm.starter.txwords;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KeyedCountUpdater extends BaseTransactionalBolt implements
		ICommitter {
	private static final long serialVersionUID = 7886850637632517015L;
	private Map<String, Integer> counts = new HashMap<String, Integer>();
	private BatchOutputCollector collector;
	TransactionAttempt id;

	int count = 0;

	public static Map<String, CountValue> COUNT_DATABASE = new HashMap<String, CountValue>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt id) {
		this.collector = collector;
		this.id = id;
	}

	@Override
	public void execute(Tuple tuple) {
		String key = tuple.getString(1);
		Integer curr = counts.get(key);
		if (curr == null) {
			curr = 0;
		}
		counts.put(key, curr + 1);
	}

	@Override
	public void finishBatch() {
		for (String key : counts.keySet()) {
			CountValue val = COUNT_DATABASE.get(key);
			CountValue newVal;
			if (val == null || !val.getTxid().equals(id)) {
				newVal = new CountValue();
				newVal.setTxid(id.getTransactionId());
				if (val != null) {
					newVal.setPrev_count(val.getCount());
					newVal.setCount(val.getCount());
				}
				newVal.setCount(newVal.getCount() + counts.get(key));
				COUNT_DATABASE.put(key, newVal);
			} else {
				newVal = val;
			}
			collector.emit(new Values(id, key, newVal.getCount(), newVal
					.getPrev_count()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "key", "count", "prev-count"));
	}

}
