package ztwu.flink;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import ztwu.flink.domain.Product;

//商品sink
public class ProductHbaseSinkFunction extends AbstractHbaseSinkFunction<Product> {
	public ProductHbaseSinkFunction(){
		tableName = "t_product";
	}
	@Override
	protected void handle(Product value, Context context, HTable table) throws Exception {
		//这里的字段必须和定义表时的字段保存一致
		Integer rowkey1 = value.getProductID();
		Put put1 = new Put(Bytes.toBytes(rowkey1));
		put1.addColumn(cf, Bytes.toBytes("pid"), getByteValue(value.getProductID()));
		put1.addColumn(cf, Bytes.toBytes("pname"), getByteValue(value.getProductName()));
		put1.addColumn(cf, Bytes.toBytes("pt"), getByteValue(value.getProductType()));
		table.put(put1);

	}
}