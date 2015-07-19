package com.cyl.storm.starter.manualdrpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.DRPCSpout;
import backtype.storm.drpc.ReturnResults;
import backtype.storm.topology.TopologyBuilder;

public class ManualDRPC {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		LocalDRPC drpc = new LocalDRPC();

		DRPCSpout spout = new DRPCSpout("exclamation", drpc);
		builder.setSpout("drpc", spout);
		builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping(
				"drpc");
		builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping(
				"exclaim");

		LocalCluster cluster = new LocalCluster();
		Config conf = new Config();
		cluster.submitTopology("exclaim", conf, builder.createTopology());

		System.out.println(drpc.execute("exclamation", "aaa"));
		System.out.println(drpc.execute("exclamation", "bbb"));

		cluster.shutdown();
		drpc.shutdown();
	}
}
