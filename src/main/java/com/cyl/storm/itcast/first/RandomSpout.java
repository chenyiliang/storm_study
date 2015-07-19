package com.cyl.storm.itcast.first;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomSpout extends BaseRichSpout {
	/**
	 * 
	 */
	private static final long serialVersionUID = -575399064818675136L;

	private static final String[] goods = { "iphone", "xiaomi", "meizu",
			"zhongxing", "huawei", "moto", "sumsung", "simens" };

	private SpoutOutputCollector collector;

	@Override
	public void nextTuple() {
		Random random = new Random();
		String good = goods[random.nextInt(goods.length)];

		collector.emit(new Values(good));
	}

	@Override
	public void open(Map map, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("src_word"));
	}

}
