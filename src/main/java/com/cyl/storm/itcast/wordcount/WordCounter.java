package com.cyl.storm.itcast.wordcount;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {
	private static final long serialVersionUID = -6435467698962564578L;
	private HashMap<String, Integer> counters = new HashMap<String, Integer>();

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context) {
		final long timeOffset = Long.parseLong(conf.get("TIME_OFFSET")
				.toString());
		new Thread(new Runnable() {
			@Override
			public void run() {
				while (true) {
					for (Entry<String, Integer> entry : counters.entrySet()) {
						System.out.println(entry.getKey() + " : "
								+ entry.getValue());
					}
					System.out
							.println("---------------------------------------");
					try {
						Thread.sleep(timeOffset * 1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}).start();
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String str = tuple.getString(0);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
