package cn.spark.analysis.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import cn.spark.analysis.bean.SessionDetail;
import cn.spark.analysis.bean.UserInfo;
import cn.spark.analysis.bean.UserVisiteAction;
import cn.spark.analysis.dao.ISessionDetailDAO;
import cn.spark.analysis.dao.IUserInfoDAO;
import cn.spark.analysis.dao.IUserVisitActionDAO;
import cn.spark.analysis.dao.impl.SessionDetailDAOImpl;
import cn.spark.analysis.dao.impl.UserInfoDAOImpl;
import cn.spark.analysis.dao.impl.UserVisitActionDAOImpl;
import cn.spark.analysis.util.DateUtils;
import cn.spark.analysis.util.StringUtils;

/**
 * 模拟数据程序
 * @author Administrator
 *
 */
public class MockData {

	/**
	 * 模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc, SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();
		
		String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
				"呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
		
		//生成的数据日期为当天日期，设置搜索参数时应注意搜索时间范围
		String date = DateUtils.getTodayDate();
		String[] actions = new String[]{"search", "click", "order", "pay"};
		Random random = new Random();
		
		for(int i = 0; i < 100; i++) {
			long userid = random.nextInt(100);    
			
			for(int j = 0; j < 10; j++) {
				String sessionid = UUID.randomUUID().toString().replace("-", "");  
				String baseActionTime = date + " " + random.nextInt(23);
				  
				for(int k = 0; k < random.nextInt(100); k++) {
					long pageid = random.nextInt(10);    
					String actionTime = baseActionTime + ":" + StringUtils.fulfuill(
							String.valueOf(random.nextInt(59))) + ":"
							+ StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
					String searchKeyword = null;
					Long clickCategoryId = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					String action = actions[random.nextInt(4)];
					if("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];   
					} else if("click".equals(action)) {
						clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));    
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));  
					} else if("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));  
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));  
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					Row row = RowFactory.create(date, userid, sessionid, 
							pageid, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds);
					rows.add(row);
				}
			}
		}
		
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true)));
		
		DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
		
		df.registerTempTable("user_visit_action"); 
		int count = (int) df.count();
		for(Row row : df.take(5)) {
			System.out.println(row +"--" + row.length() +"--"+row.getLong(1));  
		}
		for(Row row : df.take(count)) {
			UserVisiteAction userVisiteAction = new UserVisiteAction();
			userVisiteAction.date = row.getString(0);
			userVisiteAction.userId = row.getLong(1);
			userVisiteAction.sessionId = row.getString(2);
			userVisiteAction.pageId = row.getLong(3);
			userVisiteAction.actionTime = row.getString(4);
			userVisiteAction.searchKeyWord = row.getString(5);
			userVisiteAction.clickCategoryId = row.getLong(6);
			userVisiteAction.clickProductId = row.getLong(7);
			userVisiteAction.orderCategoryIds = row.getString(8);
			userVisiteAction.orderProductIds = row.getString(9);
			userVisiteAction.payCategoryIds = row.getString(10);
			userVisiteAction.payProductIds = row.getString(11);
			
			IUserVisitActionDAO userVisitActionDAO = new UserVisitActionDAOImpl();
			userVisitActionDAO.insert(userVisiteAction);
			
			SessionDetail sessionDetail = new SessionDetail();
			sessionDetail.taskId = 1;
			sessionDetail.userId = row.getLong(1);
			sessionDetail.sessionId = row.getString(2);
			sessionDetail.pageId = row.getLong(3);
			sessionDetail.actionTime = row.getString(4);
			sessionDetail.searchKeyWord = row.getString(5);
			sessionDetail.clickCategoryId = row.getLong(6);
			sessionDetail.clickProductId = row.getLong(7);
			sessionDetail.orderCategoryIds = row.getString(8);
			sessionDetail.orderProductIds = row.getString(9);
			sessionDetail.payCategoryIds = row.getString(10);
			sessionDetail.payProductIds = row.getString(11);
			
			ISessionDetailDAO sessionDetailDAO = new SessionDetailDAOImpl();
			sessionDetailDAO.insert(sessionDetail);
		}
		
		/**
		 * [2019-01-20,19,3b90d12dc1b64b5f9e416ae5e6da880e,7,2019-01-20 20:39:42,null,83,80,null,null,null,null]
			[2019-01-20,19,3b90d12dc1b64b5f9e416ae5e6da880e,3,2019-01-20 20:50:07,null,74,70,null,null,null,null]
			[2019-01-20,19,3b90d12dc1b64b5f9e416ae5e6da880e,5,2019-01-20 20:47:02,null,null,null,null,null,93,14]
			[2019-01-20,19,3b90d12dc1b64b5f9e416ae5e6da880e,5,2019-01-20 20:48:02,null,null,null,null,null,79,93]
			[2019-01-20,19,3b90d12dc1b64b5f9e416ae5e6da880e,2,2019-01-20 20:27:46,新辣道鱼火锅,null,null,null,null,null,null]
		 */
		
		/**
		 * ==================================================================
		 */
		
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user" + i;
			String name = "name" + i;
			int age = random.nextInt(60);
			String professional = "professional" + random.nextInt(100);
			String city = "city" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];
			
			Row row = RowFactory.create(userid, username, name, age, 
					professional, city, sex);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));
		
		DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		int count2 = (int) df2.count();
		for(Row row : df2.take(5)) {
			System.out.println(row+"--"+row.length()+"--"+row.getString(1));
		}
		for(Row row : df2.take(count2)) {
			UserInfo userInfo = new UserInfo();
			userInfo.userId = row.getLong(0);
			userInfo.userName = row.getString(1);
			userInfo.name = row.getString(2);
			userInfo.age = row.getInt(3);
			userInfo.professional = row.getString(4);
			userInfo.city = row.getString(5);
			userInfo.sex = row.getString(6);
			
			IUserInfoDAO userInfoDAO = new UserInfoDAOImpl();
			userInfoDAO.insert(userInfo);
		}
		/**
		 * [0,user0,name0,1,professional51,city16,male]
			[1,user1,name1,47,professional52,city99,male]
			[2,user2,name2,18,professional34,city45,male]
			[3,user3,name3,46,professional19,city24,female]
			[4,user4,name4,6,professional21,city1,female]
		 */
		
		//      session_detail:10970		                    user_info:100
		System.out.println("session_detail:"+df.count() +"		user_info:" +df2.count());
		
		df2.registerTempTable("user_info");  
	}
	
}
