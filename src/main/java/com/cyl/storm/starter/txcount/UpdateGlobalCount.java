package com.cyl.storm.starter.txcount;

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

public class UpdateGlobalCount extends BaseTransactionalBolt implements
		ICommitter {
	private static final long serialVersionUID = 4428732787670082240L;

	private TransactionAttempt attempt;
	private BatchOutputCollector collector;
	private int sum = 0;

	public static Map<String, Value> DATABASE = new HashMap<String, Value>();
	public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt attempt) {
		this.collector = collector;
		this.attempt = attempt;
	}

	@Override
	public void execute(Tuple tuple) {
		this.sum += tuple.getInteger(1);
	}

	@Override
	public void finishBatch() {
		Value val = DATABASE.get(GLOBAL_COUNT_KEY);
		Value newval;
		if (val == null || !val.getTxid().equals(attempt.getTransactionId())) {
			newval = new Value();
			newval.setTxid(attempt.getTransactionId());
			if (val == null) {
				newval.setCount(sum);
			} else {
				newval.setCount(sum + val.getCount());
			}
			DATABASE.put(GLOBAL_COUNT_KEY, newval);
		} else {
			newval = val;
		}
		collector.emit(new Values(attempt, newval.getCount()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "sum"));
	}

}
