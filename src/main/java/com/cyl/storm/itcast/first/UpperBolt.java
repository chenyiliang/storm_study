package com.cyl.storm.itcast.first;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UpperBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4098709815762260478L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String src_word = tuple.getString(0);
		String upper_word = src_word.toUpperCase();
		collector.emit(new Values(upper_word));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("upper_word"));
	}

}
