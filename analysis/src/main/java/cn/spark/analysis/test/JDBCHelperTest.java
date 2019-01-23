package cn.spark.analysis.test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import cn.spark.analysis.jdbc.JDBCHelper;
import cn.spark.analysis.jdbc.JDBCHelper.QueryCallback;

public class JDBCHelperTest {

	public static void main(String[] args) {

		//测试单条插入
		JDBCHelper helper = JDBCHelper.getInstance();
		int ctn = helper.executeUpdate("insert into test_user(name,age) values(?,?)", new Object[]{"王二",28});
		System.out.println(ctn);
		
		//测试查询
		final Map<String, Object> testUser = new HashMap<String, Object>();
		
		helper.executeQuery("select name,age from test_user where name=?", new Object[]{"小明"}, new QueryCallback() {
			
			public void process(ResultSet rs) throws Exception {
				while (rs.next()) {
					String name = rs.getString(1);
					int age = rs.getInt(2);
					testUser.put("name", name);
					testUser.put("age", age);
				}
			}
		});
		
		System.out.println(testUser.get("name") + "--" + testUser.get("age"));
		
		//测试批量插入
		String sql = "insert into test_user(name,age) value(?,?)";
		ArrayList<Object[]> paramsList = new ArrayList<Object[]>();
		paramsList.add(new Object[]{"小花",23});
		paramsList.add(new Object[]{"小白",24});
		paramsList.add(new Object[]{"小黑",25});
		
		helper.executeBatch(sql, paramsList);
	}
}
