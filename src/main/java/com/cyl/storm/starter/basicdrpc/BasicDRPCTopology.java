package com.cyl.storm.starter.basicdrpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

@SuppressWarnings("deprecation")
public class BasicDRPCTopology {

	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"exclamation");
		builder.addBolt(new ExclaimBolt(), 3);

		Config conf = new Config();

		if (args == null || args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();

			cluster.submitTopology("drpc-demo", conf,
					builder.createLocalTopology(drpc));

			for (String word : new String[] { "hello", "goodbye" }) {
				System.out.println("Result for \"" + word + "\": "
						+ drpc.execute("exclamation", word));
			}

			cluster.shutdown();
			drpc.shutdown();
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
					builder.createRemoteTopology());
		}
	}

}
