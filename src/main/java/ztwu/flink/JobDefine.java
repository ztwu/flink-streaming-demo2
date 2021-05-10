package ztwu.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import ztwu.flink.domain.Order;
import ztwu.flink.domain.Product;
import ztwu.flink.domain.User;

import java.util.Properties;

public class JobDefine {
	//用户数据直接保存hbase
	public static void userJob(StreamExecutionEnvironment env, Properties props){
		//获取kafka对应的source流
		SingleOutputStreamOperator<String> userSource = env.addSource(new FlinkKafkaConsumer010<>("user表对应的topic", new SimpleStringSchema(), props));
		//将kafka读取的数据（为json字符串SimpleStringSchema）转换成Java对象 自定义类，这里必须调用.returns(User.class)，不然启动会报错
		SingleOutputStreamOperator<User> userMessageStream=userSource.flatMap(new MyKafkaRichFlatMapFunction<User>()).returns(User.class);
		//将Java对象保存到hbase中
		userMessageStream.addSink(new UserHbaseSinkFunction()).name("UserHbaseSinkFunction");
	}
	//商品数据直接保存hbase
	public static void productJob(StreamExecutionEnvironment env,Properties props){
		//获取kafka对应的source流
		SingleOutputStreamOperator<String> productSource = env.addSource(new FlinkKafkaConsumer010<>("product表对应的topic", new SimpleStringSchema(), props));
		//将kafka读取的数据（为json字符串SimpleStringSchema）转换成Java对象 自定义类，这里必须调用.returns(Product.class)，不然启动会报错
		SingleOutputStreamOperator<Product> productStream=productSource.flatMap(new MyKafkaRichFlatMapFunction<Product>()).returns(Product.class);
		//将Java对象保存到hbase中
		productStream.addSink(new ProductHbaseSinkFunction()).name("ProductHbaseSinkFunction");
	}
	//订单数据需要进行计算统计
	public static void orderJob(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, Properties props){
		SingleOutputStreamOperator orderSource = env.addSource(new FlinkKafkaConsumer010<>("order表对应的topic", new SimpleStringSchema(), props));
		//将kafka读取的数据（为json字符串SimpleStringSchema）转换成Java对象 自定义类，这里必须调用.returns(Order.class)，不然启动会报错
		SingleOutputStreamOperator<Order> orderMessageStream = orderSource.flatMap(new MyKafkaRichFlatMapFunction<Order>()).returns(Order.class);
		//将当前订单流注册到flinktable 中，第一个参数就是表名，第二个参数就是数据流
		tEnv.createTemporaryView("t_order", orderMessageStream);
		//将order数据进行实时计算并将结果保存到MySQL中
		new OrderFunction().handle(orderMessageStream, env,tEnv);
	}
}