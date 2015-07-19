package com.cyl.storm.itcast.first;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class SuffixBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 200857435806788821L;
	private FileWriter fileWriter;

	// private static final FileWriter fileWriter;

	// static {
	// try {
	// fileWriter = new FileWriter("/data/storm/tmp.data");
	// } catch (IOException e) {
	// throw new RuntimeException(e);
	// }
	// }

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		try {
			// 要事先建立好/data/storm/文件夹
			this.fileWriter = new FileWriter("D:/data/storm/tmp-"
					+ System.currentTimeMillis() + ".data");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String upper_word = tuple.getString(0);
		String result = upper_word + "_suffix";
		try {
			fileWriter.append(result);
			fileWriter.append("\r\n");
			fileWriter.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		super.cleanup();
		if (fileWriter != null) {
			try {
				fileWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
