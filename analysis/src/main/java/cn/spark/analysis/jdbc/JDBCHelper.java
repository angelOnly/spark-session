package cn.spark.analysis.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import cn.spark.analysis.conf.ConfManager;
import cn.spark.analysis.constants.Constants;

/**
 * JDBC辅助组件 
 * 1.默认加载指定的数据库驱动，只需要修改配置文件的JDBC Driver
 * 2.实现了JDBCHelper的单例模式，保证多线程并发访问的安全 
 * 3.实现了指定数量的数据库连接池，实现了获取数据库连接的方法 
 * 4.实现增删改查的方法
 * 5.实现支持增量执行SQL的方法
 * 
 * @author dali
 *
 */
public class JDBCHelper {

	// 在静态代码块中，直接加载数据库的驱动
	static {
		try {
			String driver = ConfManager.getProperty(Constants.JDBC_DRIVER);
			Class.forName(driver);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	}

	// 数据库连接池
	private LinkedList<Connection> datasource = new LinkedList<Connection>();

	private JDBCHelper() {
		int datasourcesize = ConfManager.getInteger(Constants.JDBC_DATASORCE_SIZE);
		for (int i = 0; i < datasourcesize; i++) {
			String jdbc_url = ConfManager.getProperty(Constants.JDBC_URL);
			String jdbc_user = ConfManager.getProperty(Constants.JDBC_USER);
			String jdbc_pwd = ConfManager.getProperty(Constants.JDBC_PWD);
			
			try {
				Connection conn = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_pwd);
				datasource.push(conn);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	// 可能有时候数据库连接池中没有连接可用，实现一个简单的等待机制
	// 不断循环判断数据库连接池是否为null，为空每隔一秒等待一次，直到不为0，返回一个连接
	// 避免多线程并发问题，采用加锁机制
	public synchronized Connection getConnection() {
		while (datasource.size() == 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return datasource.poll();
	}

	private static JDBCHelper instance = null;

	public static JDBCHelper getInstance() {
		if (instance == null) {
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}

	/**
	 * 增删改的SQL执行
	 * 
	 * @param sql
	 * @param params
	 * @return
	 */
	public int executeUpdate(String sql, Object[] params) {
		int rtcn = 0;
		Connection conn = null;
		PreparedStatement statement = null;

		try {
			conn = getConnection();
			statement = conn.prepareStatement(sql);

			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}

			rtcn = statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				datasource.push(conn);
			}
		}
		return rtcn;
	}

	/**
	 * 查询的SQL执行
	 * 
	 * @param sql
	 * @param params
	 * @param callback
	 * @return
	 */
	public void executeQuery(String sql, Object[] params, QueryCallback callback) {
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet rs = null;

		try {
			conn = getConnection();
			statement = conn.prepareStatement(sql);

			for (int i = 0; i < params.length; i++) {
				statement.setObject(i + 1, params[i]);
			}

			rs = statement.executeQuery();
			callback.process(rs);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				datasource.push(conn);
			}
		}
	}

	/**
	 * 批量执行SQL的方法，每条SQL语句影响的行数 默认情况下每次执行依据SQL就会通过网络连接向MySQL发送一次请求
	 * 
	 * 如果在短时间内要执行多条结构完全一模一样的SQL，只是参数不同
	 * 虽然使用PreparedStatement这种方式，可以只编译一次，提高性能，但是对于每次的SQL，都要向MySQL发送一次网络请求
	 * 
	 * 可以通过批量执行SQL语句的功能来优化这个功能，如100条，1000条，甚至上万条 执行的时候，也仅仅编译一次
	 * 
	 * 这种批量执行SQL的语句方式，可大大提高性能
	 * 
	 * @param sql
	 * @param paramsList
	 * @return
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int rtcn[] = null;
		Connection conn = null;
		PreparedStatement statement = null;

		try {
			conn = getConnection();
			// 1. 取消Connection自动提交请求
			conn.setAutoCommit(false);
			statement = conn.prepareStatement(sql);

			// 2.使用statement.addBatch()加上批量参数
			for (Object[] params : paramsList) {
				for (int i = 0; i < params.length; i++) {
					statement.setObject(i + 1, params[i]);
				}
				// 加入批量SQL对应的参数
				statement.addBatch();
				// 3.执行批量SQL语句
				rtcn = statement.executeBatch();
				System.out.println(rtcn);
			}
			// 4.最后使用Connection的commit来一起提交
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rtcn;
	}

	/**
	 * 查询回调接口
	 * 
	 * @author dali
	 */
	public interface QueryCallback {
		void process(ResultSet rs) throws Exception;
	}
}
