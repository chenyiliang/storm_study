package com.cyl.storm.starter.txwords;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Bucketize extends BaseBasicBolt {
	private static final long serialVersionUID = 899260966740253806L;

	public static final int BUCKET_SIZE = 10;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		TransactionAttempt attempt = (TransactionAttempt) tuple.getValue(0);
		int curr = tuple.getInteger(2);
		Integer prev = tuple.getInteger(3);

		int currBucket = curr / BUCKET_SIZE;
		Integer prevBucket = null;
		if (prev != null) {
			prevBucket = prev / BUCKET_SIZE;
		}

		if (prevBucket == null) {
			collector.emit(new Values(attempt, currBucket, 1));
		} else if (currBucket != prevBucket) {
			collector.emit(new Values(attempt, currBucket, 1));
			collector.emit(new Values(attempt, prevBucket, -1));
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("attempt", "bucket", "delta"));
	}

}
