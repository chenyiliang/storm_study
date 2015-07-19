package com.cyl.storm.starter.txcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("deprecation")
public class TransactionalGlobalCount {
	public static final int PARTITION_TAKE_PER_BATCH = 3;
	@SuppressWarnings("serial")
	public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {
		{
			put(0, new ArrayList<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("chicken"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
				}
			});
			put(1, new ArrayList<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
					add(new Values("banana"));
				}
			});
			put(2, new ArrayList<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
				}
			});
		}
	};

	public static void main(String[] args) throws InterruptedException {
		MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA,
				new Fields("word"), PARTITION_TAKE_PER_BATCH);
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder(
				"global-count", "spout", spout, 3);
		builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping(
				"spout");
		builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping(
				"partial-count");

		LocalCluster cluster = new LocalCluster();

		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(3);

		cluster.submitTopology("global-count-topology", config,
				builder.buildTopology());

		Thread.sleep(3000);
		cluster.shutdown();
	}

}
