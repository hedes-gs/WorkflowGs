package com.gsphotos.storms.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class ImageKafkaBolt implements IRichBolt {

	private static final long serialVersionUID = 1L;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

	}

	@Override
	public void execute(Tuple input) {

	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}