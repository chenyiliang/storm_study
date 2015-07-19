package com.cyl.storm.itcast.first;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

public class TopoMain {
	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("randomspout", new RandomSpout(), 4);
		topologyBuilder.setBolt("upperbolt", new UpperBolt(), 4)
				.shuffleGrouping("randomspout");
		topologyBuilder.setBolt("suffixbolt", new SuffixBolt(), 4)
				.shuffleGrouping("upperbolt");

		StormTopology topology = topologyBuilder.createTopology();

		Config conf = new Config();
		// conf.setNumWorkers(4);
		conf.setDebug(true);
		// conf.setNumAckers(0);

		// StormSubmitter.submitTopology("demo", conf, topology);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("demo", conf, topology);
	}
}
