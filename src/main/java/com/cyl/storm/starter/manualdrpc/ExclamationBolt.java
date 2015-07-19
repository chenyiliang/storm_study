package com.cyl.storm.starter.manualdrpc;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 4610001481052866712L;

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String arg = tuple.getString(0);
		Object retInfo = tuple.getValue(1);
		collector.emit(new Values(arg + "!!!", retInfo));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("result", "return-info"));
	}

}
