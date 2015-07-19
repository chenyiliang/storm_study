package com.cyl.storm.starter.basicdrpc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclaimBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -8169402087283295150L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String input = tuple.getString(1);
		collector.emit(new Values(tuple.getValue(0), input + "!"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

}
