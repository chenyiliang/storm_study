package com.cyl.storm.itcast.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader extends BaseRichSpout {
	private static final long serialVersionUID = -4212883755439354646L;
	private String inputPath;

	private SpoutOutputCollector collector;

	@Override
	public void nextTuple() {
		Collection<File> files = FileUtils.listFiles(new File(inputPath),
				FileFilterUtils.notFileFilter(FileFilterUtils
						.suffixFileFilter(".bak")), null);

		for (File f : files) {
			try {
				List<String> lines = FileUtils.readLines(f, "UTF-8");
				for (String line : lines) {
					collector.emit(new Values(line));
				}
				FileUtils.moveFile(f,
						new File(f.getPath() + System.currentTimeMillis()
								+ ".bak"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		inputPath = conf.get("INPUT_PATH").toString();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}

}
