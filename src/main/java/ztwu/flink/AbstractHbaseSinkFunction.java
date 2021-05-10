package ztwu.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import ztwu.flink.domain.Product;
import ztwu.flink.domain.User;

//抽象类
public abstract class AbstractHbaseSinkFunction<OUT> extends RichSinkFunction<OUT> {
	protected static String cf_String = "f";
	protected static byte[] cf = Bytes.toBytes(cf_String);
	protected String tableName = null;
	private Connection connection;
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig()
				.getGlobalJobParameters();
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", globalParams.toMap().get("hbase.zookeeper.quorum"));
		if (null == connection) {
			connection = ConnectionFactory.createConnection(conf);
		}
	}

	@Override
	public void invoke(OUT value, Context context) throws Exception {
		HTable table = null;
		try  {
			table = (HTable) connection.getTable(TableName.valueOf(tableName));
			handle(value, context, table);
		} finally {
			table.close();
		}
	}

	protected abstract void handle(OUT value, Context context, HTable table) throws Exception;

	@Override
	public void close() throws Exception {
		connection.close();
	}

	protected byte[] getByteValue(Object value){
		if(value==null){
			return Bytes.toBytes("");
		}else{
			return Bytes.toBytes(value.toString());
		}
	}

}
