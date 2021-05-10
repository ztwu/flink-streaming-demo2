package ztwu.flink;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBUtil {
	private static Logger logger = LogManager.getLogger(DBUtil.class);
	private static DruidDataSource dataSource;

	public static DruidDataSource getDataSource(String dbUrl, String dbName, String username, String password) {
		try {
			if (dataSource == null) {
				synchronized (DBUtil.class) {
					if (dataSource == null) {
						dataSource = new DruidDataSource();
						dataSource.setDriverClassName("com.mysql.jdbc.Driver");
						dataSource.setUrl("jdbc:mysql://#/%?autoReconnect=true&useUnicode=true&useAffectedRows=true&characterEncoding=utf8".replace("#", dbUrl).replace("%", dbName));
						dataSource.setUsername(username);
						dataSource.setPassword(password);

						//configuration
						dataSource.setInitialSize(20);
						dataSource.setMinIdle(20);
						dataSource.setMaxActive(100);
						dataSource.setMaxWait(60000);
						dataSource.setTimeBetweenEvictionRunsMillis(60000);
						dataSource.setMinEvictableIdleTimeMillis(300000);
						dataSource.setValidationQuery("SELECT 1 FROM DUAL");
						dataSource.setTestWhileIdle(true);
						dataSource.setTestOnBorrow(false);
						dataSource.setTestOnReturn(false);
						dataSource.setPoolPreparedStatements(true);
						dataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
						dataSource.setFilters("stat,wall,log4j");
						dataSource.setConnectionProperties("druid.stat.mergeSql=true;druid.stat.slowSqlMillis=5000");
					}
				}
			}
		} catch (Exception e) {
			logger.error("初始化数据源信息异常....", e);
		}
		return dataSource;
	}

	/**
	 * 获取数据库连接池
	 */
	public static DataSource getDataSource() {
		return dataSource;
	}

	/**
	 * 关闭数据库连接池
	 */
	public static void closeDataSource() {
		logger.info("method datasource close !");
		if (dataSource != null) {
			dataSource.close();
		}
	}

	/**
	 * 获取数据库连接
	 */
	public static Connection getConnection() {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("获取数据库连接异常", e);
		}
		return conn;
	}

	/**
	 * 释放数据库连接
	 */
	public static void close(Connection conn, Statement st, ResultSet rs) {
		if(rs!=null) {
			try {
				rs.close();
			} catch (SQLException e) {
				logger.error("释放数据库连接异常", e);
			}
		}
		close(conn,st);
	}
	public static void close(Connection conn,Statement st) {
		if(st!=null) {
			try {
				st.close();
			} catch (SQLException e) {
				logger.error("关闭Statement异常", e);
			}
		}
		close(conn);
	}

	public static void close(Statement st) {
		if(st!=null) {
			try {
				st.close();
			} catch (SQLException e) {
				logger.error("关闭Statement异常", e);
			}
		}
	}

	public static void close(Connection conn) {
		if(conn!=null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("关闭SConnection异常", e);
			}
		}
	}
	public static void commit(Connection conn){
		if(conn!=null) {
			try {
				conn.commit();
			} catch (SQLException e) {
				logger.error("提交事务异常", e);
			}
		}
	}
	public static void rollback(Connection conn){
		if(conn!=null) {
			try {
				conn.rollback();
			} catch (SQLException e) {
				logger.error("回滚异常", e);
			}
		}
	}
}