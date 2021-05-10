package ztwu.flink;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import ztwu.flink.domain.User;

//用户sink
public class UserHbaseSinkFunction extends AbstractHbaseSinkFunction<User> {

	public UserHbaseSinkFunction(){
		tableName = "t_user";
	}
	@Override
	protected void handle(User value, Context context, HTable table) throws Exception {
		Integer rowkey1 = value.getUserID();
		Put put1 = new Put(Bytes.toBytes(rowkey1));
		//这里的字段必须和定义表时的字段保存一致
		put1.addColumn(cf, Bytes.toBytes("uid"), getByteValue(value.getUserID()));
		put1.addColumn(cf, Bytes.toBytes("uname"), getByteValue(value.getUserName()));
		put1.addColumn(cf, Bytes.toBytes("sex"), getByteValue(value.getSex()));
		put1.addColumn(cf, Bytes.toBytes("addr"), getByteValue(value.getAddress()));
		table.put(put1);
	}
}