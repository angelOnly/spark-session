package cn.spark.analysis.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import cn.spark.analysis.jdbc.JDBCHelper;

public class jdbcCURD {

	public static void main(String[] args) {

		/**
		 * JDBC基本使用过程 1.加载驱动类:Class.forName() 2.获取数据库连接:DriverManager.getConnection()
		 * 3.创建SQL执行句柄:conn.createStatement() 4.执行SQL语句:statement.executeUpdate(sql)
		 * 5.释放数据库资源:finally, connection.close()
		 */

//		insert();
//		update();
//		delete();
//		query();
//		preparedStatement();
		
		executeUpdate("insert into test_user(name,age) values(?,?)",new Object[]{"王二",28});
	}

	public static void insert() {
		// 数据库连接对象
		Connection conn = null;
		// SQL语句执行句柄
		Statement statement = null;
		try {
			// 1.加载数据库驱动
			// 反射机制加载JDBC驱动
			Class.forName("com.mysql.cj.jdbc.Driver");

			// 获取数据库的连接
			// 使用DriverManager.getConnection()方法获取针对数据库的连接
			// 需要给方法传入三个参数，包括url、user、password
			// 其中url就是有特定格式的数据库连接串，包括“主协议：子协议“//主机名：端口号//数据库”
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true", "root", "518834jzb");

			// 基于数据库连接对象Connection对象，创建SQL语句执行句柄Statement对象
			// Statement对象是用来基于底层的Connection代表数据库连接，允许我们通过java程序，Statement对象向MySQL数据库发送SQL语句
			// 从而实现通过发送的SQL语句执行增删改查等逻辑
			statement = conn.createStatement();

			/**
			 * 然后可以基于Statement对象执行SQL语句
			 */
			String sql = "insert into test_user(name,age) values('李四',21)";
			int rtn = statement.executeUpdate(sql);

			System.out.println("SQL语句影响了" + rtn + "行");

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void update() {
		Connection conn = null;
		Statement statement = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true", "root", "518834jzb");
			statement = conn.createStatement();
			String sql = "update test_user set age=27 where name='张三'";
			int rtn = statement.executeUpdate(sql);
			System.out.println("SQL语句影响了" + rtn + "行");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void delete() {
		Connection conn = null;
		Statement statement = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true", "root", "518834jzb");
			statement = conn.createStatement();
			String sql = "delete from test_user where name='张三'";
			int rtn = statement.executeUpdate(sql);
			System.out.println("SQL语句影响了" + rtn + "行");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	public static void query() {
		Connection conn = null;
		Statement statement = null;
		/**
		 * 查询语句需要通过 ResultSet 保存查询的数据集
		 */
		ResultSet resultSet = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true", "root", "518834jzb");
			statement = conn.createStatement();
			String sql = "select * from test_user";
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				String name = resultSet.getString(1);
				int age = resultSet.getInt(2);
				System.out.println(name + "-" + age);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (statement != null) {
					statement.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 如果使用Statement，必须在sql中嵌入值执行，有2个弊端
	 * 1.SQL注入：网页用户在使用时，如论坛留言，电商评论，论文内容等页面，可以使用 '1' or '1'，诸如此类的非法字符，
	 * 执行该Statement就会原封不动的将用户填写的内容拼接在SQL中，可能造成数据库损坏甚至数据泄露。
	 * 
	 * 2.性能低下：对于类似的SQL语句，每句都要进行编译，编译在整个SQL执行中占据大部分比例。
	 * 
	 * 使用preparedStatement，可以解决上述2个问题。
	 * 1.preparedStatement：可以在SQL中对值的位置使用 ? 占位符，我们在执行SQL之前可以对占位符内容进行处理，在内容合法的情况下执行sql
	 * 2.提升性能：结构类似的SQL只需要编译一次。
	 */
	public static void preparedStatement() {
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true", "root", "518834jzb");
			String sql = "insert into test_user(name,age) values(?,?)";
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "小红");
			pstmt.setInt(2, 22);
			int rtn = pstmt.executeUpdate();
			System.out.println("SQL语句影响了" + rtn + "行");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (pstmt != null) {
					pstmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static int executeUpdate(String sql, Object[] params) {
		int rtcn = 0;
		Connection conn = null;
		PreparedStatement statement = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?allowPublicKeyRetrieval=true", "root", "518834jzb");
			statement = conn.prepareStatement(sql);
			
			for (int i = 0; i < params.length; i++) {
				System.out.println(params[0]+"---"+params[1]);
				statement.setObject(i+1, params[i]);
			}
			
			rtcn = statement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null){
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return rtcn;
	}
}
