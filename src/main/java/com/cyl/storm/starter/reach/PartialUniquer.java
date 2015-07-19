package com.cyl.storm.starter.reach;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class PartialUniquer extends BaseBatchBolt<Object> {
	private static final long serialVersionUID = 6077653968028669810L;
	private BatchOutputCollector collector;
	private Object id;
	private Set<String> followers = new HashSet<String>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, Object id) {
		this.collector = collector;
		this.id = id;
	}

	@Override
	public void execute(Tuple tuple) {
		followers.add(tuple.getString(1));
	}

	@Override
	public void finishBatch() {
		System.err.println(id.toString());
		System.err.println(followers);
		// System.err.println(followers.hashCode());
		collector.emit(new Values(id, followers.size()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "partial-count"));
	}

}
