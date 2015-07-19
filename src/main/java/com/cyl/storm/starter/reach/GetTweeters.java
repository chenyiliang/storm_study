package com.cyl.storm.starter.reach;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class GetTweeters extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8336314991594630859L;
	public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
		private static final long serialVersionUID = -3571918952723469980L;
		{
			put("foo.com/blog/1",
					Arrays.asList("sally", "bob", "tim", "george", "nathan"));
			put("engineering.twitter.com/blog/5",
					Arrays.asList("adam", "david", "sally", "nathan"));
			put("tech.backtype.com/blog/123",
					Arrays.asList("tim", "mike", "john"));
		}
	};

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Object id = tuple.getValue(0);
		String url = tuple.getString(1);
		List<String> tweeters = TWEETERS_DB.get(url);
		if (tweeters != null) {
			for (String tweeter : tweeters) {
				collector.emit(new Values(id, tweeter));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "tweeter"));
	}

}
