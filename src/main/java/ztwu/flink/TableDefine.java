package ztwu.flink;

import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;

public class TableDefine {

	public static void defineUserHbaseTable(StreamTableEnvironment tEnv, Configuration conf){
		HBaseTableSource hBaseTableSource = new HBaseTableSource(conf, "t_user");
		//设置hbase表的rowKey及其类型
		hBaseTableSource.setRowKey("rowKey", Integer.class);
		//设置hbase表的字段及其类型 第一个参数为列簇，第二个参数为字段名(最后简写以减少存储空间和执行效率)，第三个参数为类型
		hBaseTableSource.addColumn("f", "uid", Integer.class);
		hBaseTableSource.addColumn("f", "uname", String.class);
		hBaseTableSource.addColumn("f", "sex", String.class);
		hBaseTableSource.addColumn("f", "address", String.class);
		//向flinktable注册处理函数
		// 第一个参数为注册函数名字，即flink-sql中的表名，而new HBaseTableSource(conf, "t_user")中的t_user为hbase的表名
		// 第二个参数是一个TableFunction，返回结果为要查询出的数据列，即hbase表addColumn的哪些列，参数为rowkey，表示根据rowkey即userID进行查询
		tEnv.registerFunction("t_user", hBaseTableSource.getLookupFunction(new String[]{"rowKey"}));
	}

	public static void defineProductHbaseTable(StreamTableEnvironment tEnv,Configuration conf){
		HBaseTableSource hBaseTableSource = new HBaseTableSource(conf, "t_product");
		//设置hbase表的rowKey及其类型
		hBaseTableSource.setRowKey("rowKey", Integer.class);
		//设置hbase表的字段及其类型 第一个参数为列簇，第二个参数为字段名(最后简写以减少存储空间和执行效率)，第三个参数为类型
		hBaseTableSource.addColumn("f", "pid", Integer.class);
		hBaseTableSource.addColumn("f", "pname", String.class);
		hBaseTableSource.addColumn("f", "pt", String.class);
		//向flinktable注册处理函数
		// 第一个参数为注册函数名字，即flink-sql中的表名，而new HBaseTableSource(conf, "t_product")中的t_product为hbase的表名
		// 第二个参数是一个TableFunction，返回结果为要查询出的数据列，即hbase表addColumn的哪些列，参数为rowkey，表示根据rowkey即userID进行查询
		tEnv.registerFunction("t_product", hBaseTableSource.getLookupFunction(new String[]{"rowKey"}));
	}
}