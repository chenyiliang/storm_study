package com.cyl.storm.starter.txcount;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchCount extends BaseBatchBolt<Object> {
	private static final long serialVersionUID = 8722132215887608023L;
	private Object id;
	private BatchOutputCollector collector;
	private int count = 0;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, Object id) {
		this.collector = collector;
		this.id = id;
	}

	@Override
	public void execute(Tuple tuple) {
		this.count++;
	}

	@Override
	public void finishBatch() {
		this.collector.emit(new Values(id, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "count"));
	}

}
