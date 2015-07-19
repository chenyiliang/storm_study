package com.cyl.storm.starter.reach;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;

@SuppressWarnings("deprecation")
public class ReachTopology {

	public static LinearDRPCTopologyBuilder construct() {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"reach");
		builder.addBolt(new GetTweeters(), 4);
		builder.addBolt(new GetFollowers(), 12).shuffleGrouping();
		builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(
				new Fields("id", "follower"));
		builder.addBolt(new CountAggregator(), 3).fieldsGrouping(
				new Fields("id"));
		return builder;
	}

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		LinearDRPCTopologyBuilder builder = construct();
		Config conf = new Config();

		if (args == null || args.length == 0) {
			conf.setMaxTaskParallelism(3);
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("reach-drpc", conf,
					builder.createLocalTopology(drpc));

			String[] urlsToTry = new String[] { "foo.com/blog/1",
					"engineering.twitter.com/blog/5", "notaurl.com" };
			for (String url : urlsToTry) {
				System.err.println("Reach of " + url + ": "
						+ drpc.execute("reach", url));
			}

			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createRemoteTopology());
		}
	}

}
