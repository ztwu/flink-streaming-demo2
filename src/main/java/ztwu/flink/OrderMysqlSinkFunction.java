package ztwu.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import ztwu.flink.domain.Result;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

public class OrderMysqlSinkFunction extends RichSinkFunction<List<Result>> {
	public static Logger logger = LogManager.getLogger(OrderMysqlSinkFunction.class);
	private DataSource dataSource = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		logger.info("MysqlSinkFunction open");
		super.open(parameters);
		Map<String, String> globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

		String dbUrl = globalParams.get("dbUrl");
		String dbName = globalParams.get("dbName");
		String user = globalParams.get("user");
		String psw = globalParams.get("psw");
		try {
			dataSource = DBUtil.getDataSource(dbUrl, dbName, user, psw);
		} catch (Exception e) {
			logger.error("" + e);
		}
	}

	@Override
	public void invoke(List<Result> results, Context context) throws Exception {
		Connection connection = dataSource.getConnection();
		PreparedStatement ps = null;
		try {
			//构建sql userName, productType, productName, buyDate, productCount, totalMoney
			String sql = "INSERT INTO t_result (userName, productType, productName, buyDate, productCount, totalMoney) " +
					" values (?,?,?,?,?,?) on duplicate key " +
					" update productCount = values(productCount),totalMoney=values(totalMoney) ";
			ps = connection.prepareStatement(sql);

			for (Result record : results) {
				ps.setObject(1, record.getUserName());
				ps.setObject(2, record.getProductType());
				ps.setObject(3, record.getProductName());
				ps.setObject(4, record.getBuyDate());
				ps.setObject(5, record.getProductCount());
				ps.setObject(6, record.getTotalMoney());
				ps.addBatch();
			}
			ps.executeBatch();
		}catch (Exception e){
			logger.error("数据入库异常："+e);
			throw new RuntimeException("gcoll_vmotask_sum保存数据库失败："+e);
		}finally {
			DBUtil.close(ps);
		}
	}

	@Override
	public void close() {
		logger.info("MysqlSinkFunction close");
		DBUtil.closeDataSource();
	}
}