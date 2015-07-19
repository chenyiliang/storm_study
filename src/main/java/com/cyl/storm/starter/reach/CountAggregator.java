package com.cyl.storm.starter.reach;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountAggregator extends BaseBatchBolt<Object> {
	private static final long serialVersionUID = 7920475206439023741L;

	BatchOutputCollector collector;
	Object id;
	int count = 0;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, Object id) {
		this.collector = collector;
		this.id = id;
	}

	@Override
	public void execute(Tuple tuple) {
		this.count += tuple.getInteger(1);
	}

	@Override
	public void finishBatch() {
		collector.emit(new Values(id, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "reach"));
	}

}
