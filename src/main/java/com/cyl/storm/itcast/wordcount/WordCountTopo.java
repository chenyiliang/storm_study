package com.cyl.storm.itcast.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class WordCountTopo {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err.println("Usage: inputPath timeOffset");
			System.exit(2);
		}

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-spliter", new WordSpliter()).shuffleGrouping(
				"word-reader");
		builder.setBolt("word-counter", new WordCounter()).shuffleGrouping(
				"word-spliter");

		Config conf = new Config();
		conf.put("INPUT_PATH", args[0]);
		conf.put("TIME_OFFSET", args[1]);
		// conf.setDebug(false);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordCount", conf, builder.createTopology());
	}

}
