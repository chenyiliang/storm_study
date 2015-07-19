package com.cyl.storm.my.drpc1;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//http://blog.chinaunix.net/uid-233938-id-3198826.html
@SuppressWarnings("deprecation")
public class ExclaimBolt implements IBasicBolt {
	private static final long serialVersionUID = -4869794927125819585L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String input = tuple.getString(1);
		collector.emit(new Values(tuple.getValue(0), input + "!"));
	}

	@Override
	public void cleanup() {
	}

	public static void main(String[] args) {
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(
				"exclamation");
		builder.addBolt(new ExclaimBolt(), 3);

		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();

		Config conf = new Config();
		cluster.submitTopology("drpc-demo", conf,
				builder.createLocalTopology(drpc));

		System.out.println("Results for 'hello':"
				+ drpc.execute("exclamation", "hello"));

		cluster.shutdown();
		drpc.shutdown();
	}

}
