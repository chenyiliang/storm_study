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

public class GetFollowers extends BaseBasicBolt {
	private static final long serialVersionUID = 7924854388776200439L;

	public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
		private static final long serialVersionUID = 5275405245195900940L;
		{
			put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim",
					"chris", "jai"));
			put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david",
					"vivian"));
			put("tim", Arrays.asList("alex"));
			put("nathan", Arrays.asList("sally", "bob", "adam", "harry",
					"chris", "vivian", "emily", "jordan"));
			put("adam", Arrays.asList("david", "carissa"));
			put("mike", Arrays.asList("john", "bob"));
			put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
		}
	};

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		Object id = tuple.getValue(0);
		String tweeter = tuple.getString(1);
		List<String> followers = FOLLOWERS_DB.get(tweeter);
		if (followers != null) {
			for (String follower : followers) {
				collector.emit(new Values(id, follower));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "follower"));
	}

}
