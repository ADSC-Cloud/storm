/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.metric.LoggingMetricsConsumer;
import storm.starter.spout.RandomSentenceSpout;
import backtype.storm.scheme.StringScheme;
import backtype.storm.spout.KestrelThriftSpout;
import redis.clients.jedis.Jedis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
  static final Logger LOG = LoggerFactory.getLogger(WordCountTopology.class);

  static final int NUM_BOLTS = 10;
  // static final int SLEEP_TIME = 500;
  static final int NUM_SPOUTS = 4;
  // static final int SPOUT_SLEEP_TIME = 150;
  static final int VAR_LIMIT = 4;
  static final Random random = new Random();

  static final String SLEEP_TIME_KEY = "storm-sleep-time";
  static final String SPOUT_SLEEP_TIME_KEY = "storm-spout-sleep-time";

  static long getSleepTime() {
    Jedis jedis = new Jedis("localhost");
    int sleep_time = Integer.parseInt(jedis.get(SLEEP_TIME_KEY));
    // return sleep_time;

    double t = 0;
    for (int i = 0; i < VAR_LIMIT; i++) {
      t += -Math.log(1.0 - random.nextDouble()) * sleep_time;
    }
    return Math.round(t / VAR_LIMIT);
  }

  static boolean boltSleep() {
    long s = System.currentTimeMillis();
    long sleep_time = getSleepTime();
    try {
      Thread.sleep(sleep_time);
    } catch (Exception ex) {
    }
    long t = System.currentTimeMillis();
    long d = t - s;
    LOG.info("actual sleep time: " + d + " v.s. " + sleep_time);
    return true;
    // return d > sleep_time * 0.8;
  }

  static long getSpoutSleepTime() {
    Jedis jedis = new Jedis("localhost");
    return Integer.parseInt(jedis.get(SPOUT_SLEEP_TIME_KEY));
  }

  public static class SplitSentence extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      if (!boltSleep()) return;

      String sentence = tuple.getString(0);
      collector.emit(new Values(sentence));
      // String[] words = sentence.split(" ");
      // for (int i = 0; i < words.length; i++) {
      //   collector.emit(new Values(words[i]));
      // }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      if (!boltSleep()) return;
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static class OutputBolt extends BaseBasicBolt {
    String _host;
    String _set;
    String _counter;

    public OutputBolt(String host, String set, String counter) {
      _host = host;
      _set = set;
      _counter = counter;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      if (!boltSleep()) return;
      String word = tuple.getString(0);
      collector.emit(new Values(word));

      Jedis jedis = new Jedis(_host);
      jedis.sadd(_set, word);
      jedis.incr(_counter);
      LOG.info("job completed " + word);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
  }

  public static class RedisSpout extends BaseRichSpout {
    String _queue;
    String _host;
    SpoutOutputCollector _collector;

    public RedisSpout(String host, String queue) {
      _host = host;
      _queue = queue;
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void nextTuple() {
      try {
        Thread.sleep(getSpoutSleepTime());
      } catch (Exception ex) {
      }
      Jedis jedis = new Jedis(_host);
      List<String> arr = jedis.blpop(0, _queue);
      String sentence = arr.get(1);
      LOG.info("redis lpopped " + sentence);
      // _collector.emit(new Values(sentence), sentence);
      _collector.emit(new Values(sentence));
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    // builder.setSpout("spout", new RandomSentenceSpout(), 5);
    // builder.setSpout("spout",
    //     new KestrelThriftSpout("localhost", 2229, "test_queue", new StringScheme()), 3);
    builder.setSpout("spout", new RedisSpout("localhost", "storm-wci"), NUM_SPOUTS);

    builder.setBolt("split", new SplitSentence(), NUM_BOLTS).shuffleGrouping("spout");

    // builder.setBolt("count", new WordCount(), 10).fieldsGrouping("split", new Fields("word"));
    // builder.setBolt("count", new WordCount(), 12).shuffleGrouping("split");
    builder.setBolt("count", new WordCount(), NUM_BOLTS).shuffleGrouping("split");

    builder.setBolt("output", new OutputBolt("localhost", "storm-wco", "storm-wordcount"), NUM_BOLTS)
      .shuffleGrouping("count");

    Config conf = new Config();
    conf.setDebug(true);
    // conf.registerMetricsConsumer(LoggingMetricsConsumer.class, null, 1);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(4);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
