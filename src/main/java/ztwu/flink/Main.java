package ztwu.flink;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.Map;
import java.util.Properties;

public class Main {
	public static void main(String[] args) throws Exception {
		//1. 相关参数设置
		//创建流执行环境，并创建表执行环境
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//如果需要使用sql和table的话，这个必须设置
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		//非常关键，一定要设置启动检查点,设置最少一次处理语义和恰一次处理语义
		env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
		//按照处理时间进行计算
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		// checkpoint的清除策略
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		checkpointConfig.setTolerableCheckpointFailureNumber(0);
		//设置重启策略：3次尝试，每次尝试间隔60s
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 60000));
		Map<String, String> maps = new ImmutableMap.Builder<String, String>().
				put("kafka.url", args[0]).
				put("hbase.zookeeper.quorum", args[1]).
				put("dbUrl", args[2]).
				put("dbName", args[3]).
				put("user", args[4]).
				put("psw", args[5]).build();
		//相关数据设置成全局配置，以便再后面的function中获取
		env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(maps));
		//table中时间转换函数
		tEnv.registerFunction("time_format", new DateFormatFunction());

		//2. 创建hbase环境
		//从全局配置中获取hbase的zookeeper地址
		String zkUrl = env.getConfig().getGlobalJobParameters().toMap().getOrDefault("hbase.zookeeper.quorum", "");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkUrl);

		//2.1 创建hbase表
		TableDefine.defineUserHbaseTable(tEnv, conf);//创建用户表
		TableDefine.defineProductHbaseTable(tEnv, conf);//创建商品表


		//3. 创建kafka环境
		//3.1 从全局配置中获取kafka相关配置
		String kafkaUrl = env.getConfig().getGlobalJobParameters().toMap().getOrDefault("kafka.url", "");
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", kafkaUrl);
		props.setProperty("group.id", "配置的groupID");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		//3.2 直接入库Hbase库的维度数据和需要进行实时计算的数据这里分别写了一个，因为还是有些不同的
		//3.2.1 维度数据的处理
		JobDefine.userJob(env, props);
		JobDefine.productJob(env, props);

		//实时计算的处理
		JobDefine.orderJob(env, tEnv, props);
		//执行任务
		env.execute("orderJob");
	}
}