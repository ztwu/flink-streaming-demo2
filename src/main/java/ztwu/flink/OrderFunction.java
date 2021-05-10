package ztwu.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import ztwu.flink.domain.Result;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class OrderFunction {

	public void handle(SingleOutputStreamOperator streamOperator, StreamExecutionEnvironment env,
					   StreamTableEnvironment tEnv){
		/**
		 * 这里比较坑，需要说明下：
		 * 1. 字段顺序的问题，select中的字段顺序和类型必须和Result类中一致，不然会报异常 Field types of query result and registered TableSink  do not match
		 *    flink框架会根据返回结果Result类中的属性的字典升序进行排序，并将排序完后的字段及其字段类型和select 中的字段进行类型匹配，只要类型匹配成功则成功
		 *    如下面的返回类型Result类，字段按照字典升序排列后为：[buyDate: STRING, productCount: INT, productName: STRING, productType: STRING, totalMoney: DOUBLE, userName: STRING]
		 *    那么我们的select字段的返回类型就必须为：[STRING, INT, STRING, STRING,DOUBLE,STRING]
		 *    假设我们的sql为(sum(o.productCount) ,o.buyDate交换了位置)：select sum(o.productCount) ,o.buyDate,p.pname,p.pt,sum(o.money*o.productCount) ,u.uname 则会报错Field types of query result and registered TableSink  do not match
		 *    假设我们的sql为（pt和pname交换了位置）：select o.buyDate,sum(o.productCount),p.pt,p.pname,sum(o.money*o.productCount) ,u.uname，那么Result类
		 *          的productName的值将会为pt的值，productType的值将会为pname的值
		 * 2. 对于hbase维度数据，维度数据的字段为简写，如username被写成的uname，那么这里select 中的字段必须和hbase表定义的字段一致，如p.pname,u.uname
		 * 3. 如果select中的字段类型和result类中字段类型不一样时，需要在sql中使用相关函数进行转换
		 * 4. 在这里的t_order必须和JobDefine中注册到table环境中的【视图名字】一致，如tEnv.createTemporaryView("t_order", orderMessageStream);
		 * 5. 这里的t_user必须和TableDefine中注册到table环境中的【注册函数名】 一致，如：tEnv.registerFunction("t_user", hBaseTableSource.getLookupFunction(new String[]{"rowKey"}));
		 * 6.这里的t_product必须和TableDefine中注册到table环境中的【注册函数名】 一致，如：tEnv.registerFunction("t_product", hBaseTableSource.getLookupFunction(new String[]{"rowKey"}));
		 *
		 */
		Table collectionTaskResult = tEnv.sqlQuery("select o.buyDate,sum(o.productCount) ,p.pname,p.pt,sum(o.money*o.productCount) ,u.uname" +
				" from t_order o,lateral table(t_user(o.userID)) u,lateral table(t_product(o.productID)) p " +
				" where o.userID=u.uid and o.productID=p.pid group by u.uname,p.pt,p.pname,o.buyDate");
		DataStream<Tuple2<Boolean, Result>> stream = tEnv.toRetractStream(collectionTaskResult, Result.class);
		//windAll 这里是为了减轻数据库的压力，每10秒保存一次库，在存库前将数据进行处理，只保存唯一键相同的一条数据
		stream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.process(new ProcessAllWindowFunction<Tuple2<Boolean, Result>, List<Result>, TimeWindow>() {

					//values的数量等于这个时间窗口范围内的所有数据量
					public void process(Context context, Iterable<Tuple2<Boolean, Result>> values, Collector<List<Result>> out) throws Exception {
						Iterator<Tuple2<Boolean, Result>> iterator = values.iterator();
						Map<String, Result> result = Maps.newHashMap();
						while (iterator.hasNext()) {
							Tuple2<Boolean, Result> tuple = iterator.next();
							//输入流进行收集
							if (tuple.getField(0)) {
								Result item = tuple.getField(1);
								//这样map的结果表示为唯一键相同时的最新数据
								result.put(item.getUserName()+":"+item.getProductType()+":"+item.getProductName()+":"+item.getBuyDate(), item);
							}
						}
						out.collect(Lists.newArrayList(result.values()));
					}
				}).setParallelism(1)//之所以要将并行度设置为1，是为了防止并发写入数据库导致数据错误
				//将Java对象保存到MySQL中
				.addSink(new OrderMysqlSinkFunction()).name("OrderMysqlSinkFunction");


	}
}