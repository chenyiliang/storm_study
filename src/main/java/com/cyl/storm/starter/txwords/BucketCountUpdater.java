package com.cyl.storm.starter.txwords;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BucketCountUpdater extends BaseTransactionalBolt {
	private static final long serialVersionUID = -8727636226448875260L;

	private Map<Integer, Integer> accum = new HashMap<Integer, Integer>();
	private BatchOutputCollector collector;
	private TransactionAttempt attempt;

	//private int count = 0;

	public static Map<Integer, BucketValue> BUCKET_DATABASE = new HashMap<Integer, BucketValue>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, TransactionAttempt attempt) {
		this.collector = collector;
		this.attempt = attempt;
	}

	@Override
	public void execute(Tuple tuple) {
		Integer bucket = tuple.getInteger(1);
		Integer delta = tuple.getInteger(2);
		Integer curr = accum.get(bucket);
		if (curr == null) {
			curr = 0;
		}
		accum.put(bucket, curr + delta);
	}

	@Override
	public void finishBatch() {
		for (Integer bucket : accum.keySet()) {
			BucketValue currVal = BUCKET_DATABASE.get(bucket);
			BucketValue newVal;
			if (currVal == null
					|| !currVal.getTxid().equals(attempt.getTransactionId())) {
				newVal = new BucketValue();
				newVal.setTxid(attempt.getTransactionId());
				newVal.setCount(accum.get(bucket));
				if (currVal != null) {
					newVal.setCount(newVal.getCount() + currVal.getCount());
				}
				BUCKET_DATABASE.put(bucket, newVal);
			} else {
				newVal = currVal;
			}
			collector.emit(new Values(attempt, bucket, newVal.getCount()));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "bucket", "count"));
	}

}
