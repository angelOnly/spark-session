package cn.spark.analysis.session.spark;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSONObject;

import cn.spark.analysis.bean.Task;
import cn.spark.analysis.conf.ConfManager;
import cn.spark.analysis.constants.Constants;
import cn.spark.analysis.dao.ITaskDAO;
import cn.spark.analysis.dao.impl.DaoFactory;
import cn.spark.analysis.test.MockData;
import cn.spark.analysis.util.ParamUtils;
import cn.spark.analysis.util.StringUtils;
import cn.spark.analysis.util.ValidUtils;
import scala.Tuple2;

/**
 * 用户访问Session分析
 * 
 * 接收用户创建的分析任务，用户可以指定的条件如下：
 * 1. 时间范围：起始日期 - 结束日期
 * 2. 性别：男，女
 * 3. 年龄范围：20-50岁
 * 4. 职业:多选
 * 5. 城市：多选
 * 6. 搜索词：多个搜索词，只要某个session中任何一个action搜索过指定的关键词，那么session久符合条件
 * 7. 点击品类：多个品类，只要某个session中任何一个action点击过某个品类，那么session久符合条件
 * 2-7：数据均在我们要聚合的数据中
 * 
 * Spark作业如何接受用户创建的任务？
 * 1. J2EE平台在接受用户创建的任务请求之后，会将任务信息插入MySQL的task表中，任务参数以JSON格式封装在task_param字段中。
 * 2. J2EE平台会执行spark-submit shell脚本，并将taskid作为参数传递给spark-submit shell脚本，
 * 	  spark-submit shell脚本会将参数传递给spark作业的main函数。
 */
public class SessionAnalyze {

	public static void main(String[] args) {
		args = new String[]{"1"};
		SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME).setMaster(Constants.MASTER_LOCAL);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		//生成模拟测试数据
		mockData(sc, sqlContext);
		
		//创建需要使用的DAO组件
		ITaskDAO taskDAO = DaoFactory.getTaskDAO();
		
		//查询指定任务，并获取任务的查询参数
		long taskId = ParamUtils.getTaskIdFromArgs(args);
		Task task = taskDAO.findTaskById(taskId);
		JSONObject taskParam = JSONObject.parseObject(task.taskParam);
		
		//如果要进行session粒度的数据聚合，首先要从user_visit_action表中，查询出指定日期范围内的行为数据
		//首先要根据用户在创建任务时指定的参数，来进行数据过滤和筛选
		JavaRDD<Row> actionRDD = getActionRDDByDataRange(sqlContext, taskParam);
		
		//首先将行为数据按照session_id进行groupById分组，此时的数据就是session粒度了
		//然后将session粒度数据与用户信息行为数据进行join，然后就可以获取到session粒度的数据
		//同时数据里还包含session对应的user信息
		
		/**
		 * 1.查询出指定范围内的数据 getActionRDDByDataRange()
		 * 2.将数据映射成 <sessionId,Row> 格式，对行为数据按session粒度进行分组 aggregateBySession()
		 * 3.对每个session分组进行聚合，将session中所有搜索词和点击品类都聚合起来，
		 * 	 并将搜索词和点击品类id连接到一个字符串中，最后将sessionId，searchKeywords,clickCategoryIds 都已指定的格式连接到一个字符串 partAggrInfo
		 *   再返回 sessionId2PartAggrInfoRDD <userId, partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		 * 4.查询出所有user，将user数据映射成<userId,Row>
		 * 5.将之前聚合的sessionId2PartAggrInfoRDD和user数据进行join
		 * 6.对session的部分数据(搜索词和品类id)和user的age，professional，city，sex等连接成字符串,fullAggrInfo
		 * 7.最后返回完整的session数据<sessionId,fullAggrInfo>
		 */
		
		//到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>
		JavaPairRDD<String, String> sessionId2AggrInfoRDD = aggregateBySession(actionRDD, sqlContext);
		
		//985 
		System.out.println("session聚合数据："+sessionId2AggrInfoRDD.count());
		for (Tuple2<String, String> tuple : sessionId2AggrInfoRDD.take(10)) {
			System.out.println(tuple._1+":"+tuple._2);
		}
		
		//接着就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		JavaPairRDD<String, String> filterdSessionid2AggrInfoRDD = filterSession(sessionId2AggrInfoRDD, taskParam);
		
		//653
		System.out.println("session过滤数据："+filterdSessionid2AggrInfoRDD.count());
		for (Tuple2<String, String> tuple : filterdSessionid2AggrInfoRDD.take(10)) {
			System.out.println(tuple._1+":"+tuple._2);
		}
		
		/**
		 * Session统计聚合(统计出访问时长和访问步长，各个区间的Session数量占总Session数量的比例)
		 * 
		 * 如果不重构，直接实现，思路：
		 * 1.actionRDD做映射成<sessionid,Row>格式
		 * 2.按Sessionid聚合，计算出每个Session的访问时长和访问步长
		 * 3.遍历新生成的RDD，将每个Session的访问时长和访问步长去更新自定义Accumulator中自定义的值
		 * 4.使用自定义Accumulator中的统计值，去计算各个区间的比例
		 * 5.将最后计算出来的结果写入MySQL对应的表中
		 * 
		 * 普通实现思路的问题：
		 * 1.actionRDD在之前Session聚合的时候已经映射过，这里显得多余。
		 * 2.没有必要为了Session聚合这个功能单独遍历一遍Session。
		 * 
		 * 重构实现思路
		 * 1.不要去生成新的RDD(可能要处理上亿的数据)
		 * 2.不要去单独遍历一遍Session的数据(处理上千万的数据)
		 * 3.可以在进行Session聚合时，就直接计算出每个Session的访问时长和访问步长
		 * 4.在进行过滤的时候，本来就要遍历所有的聚合Session信息，此时，就可以在某个Session通过筛选条件后，将其访问时长和访问步长累加到自定义Accumulator上
		 * 5.在这两种截然不同的实现方式上，面对上亿，上千万数据时，甚至可以节省时间长达半小时甚至几小时
		 * 
		 * 开发Spark大型项目的经验准则：
		 * 1.尽量少生成RDD
		 * 2.尽量少对RDD进行算子操作，如果有可能，尽量在一个算子里面，实现多个算子功能
		 * 3.尽量少对RDD进行shuffle算子操作，groupByKey，sortByKey，reduceByKey，shuffle操作会导致大量磁盘读写，导致严重性能下降
		 *   shuffle算子很容易导致数据倾斜，一旦倾斜，就是性能杀手
		 * 4.无论什么功能，性能第一  
		 * 
		 * 第一种方式：解耦，易维护，设计优先
		 * 第二种：性能优先
		 */
		
		sc.close();
	}
	
	/**
	 * 获取SQLContext
	 * 如果是本地测试环境，就生成SQLContext对象
	 * 如果是生产环境，就生成HiveContext对象
	 */
	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);
		}else {
			return new HiveContext(sc);
		}
	}
	
	/**
	 * 生成模拟数据
	 * 只有是本地模式，才会生成模拟数据
	 * 生产环境，直接读取的是hive表中的数据
	 * @param sc
	 * @param sqlContext
	 */
	private static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
		boolean local = ConfManager.getBoolean(Constants.SPARK_LOCAL);
		if(local) {
			MockData.mock(sc, sqlContext);
		}
	}
	
	/**
	 * 获取指定日期范围的用户访问行为数据
	 * @param sqlContext
	 * @param taskParams
	 * @return
	 */
	private static JavaRDD<Row> getActionRDDByDataRange(SQLContext sqlContext, JSONObject taskParam) {
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		String sql = "select * from user_visit_action where data>='"+startDate+"' and date<='"+endDate+"'";
		
		DataFrame actionDF = sqlContext.sql(sql);
		return actionDF.javaRDD();
	}
	
	/**
	 * 对行为数据按session粒度进行聚合
	 * @param actionRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SQLContext sqlContext) {
		//actionRDD中的元素是Row，一个Row就是一行用户访问行为记录（点击，搜索）
		//将Row映射成<sessionId,Row>格式
		JavaPairRDD<String,Row> sessionId2ActionRDD = actionRDD.mapToPair(t -> new Tuple2<String, Row>(t.getString(2), t));
		
		//对行为数据按session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionId2ActionsRDD = sessionId2ActionRDD.groupByKey();
		
		//对每个session分组进行聚合，将session中所有搜索词和点击品类都聚合起来
		//到此为止，获取的数据格式：<userId, partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long,String>  sessionId2PartAggrInfoRDD = sessionId2ActionsRDD.mapToPair(t -> {
			String sessionId = t._1;
			Iterator<Row> iterator = t._2.iterator();
			
			Long userid = null;
			
			StringBuffer searchKeyWordsBuffer = new StringBuffer();
			StringBuffer clickCategoryIdsBuffer = new StringBuffer();
			
			//遍历session所有用户行为
			while (iterator.hasNext()) {
				//提取每个访问行为的搜索词字段和点击品类字段行为
				Row row = iterator.next();
				String searchKeyWord = row.getString(5);
				Long clickCategoryId = row.getLong(6);
				
				if (userid == null) {
					userid = row.getLong(1);
				}
				
				//并不是每一行访问行为都有searchKeyWords和clickCategoryIds
				//只有搜索行为有searchKeyWords，只有点击品类行为有clickCategoryIds
				//所以任何一行行为都不可能2个字段都有的，所以数据是可能出现null值的
				
				//我们决定是否将搜索词或点击品类id拼接到字符串中去
				//需要满足：不能是null值，之前的字符串中还没有搜索词或点击品类id
				if (StringUtils.isEmpty(searchKeyWord)) {
					if (searchKeyWordsBuffer.toString().contains(searchKeyWord)) {
						searchKeyWordsBuffer.append(searchKeyWord+",");
					}
				}
				if (clickCategoryId != null) {
					if (clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
						clickCategoryIdsBuffer.append(clickCategoryId+",");
					}
				}
			}
			
			String searchKeyWords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
			String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
			
			//返回的数据格式是<sessionId,partAggrInfo>，这一步聚合玩之后，还需要将每一行数据，跟对应的用户信息进行聚合
			//问题来了，如果跟用户信息进行聚合，那么key就不应该是sessionId，而是userId
			//才能跟<userId,Row>格式的用户信息进行聚合
			//如果这里直接返回<sessionId,partAggrInfo>,还需要再做一次mapToPair算子
			//将RDD映射成<userId,Row>格式，显得多余
			
			//所以这里直接返回的数据格式为<userId,partAggrInfo>，然后跟用户信息join的时候，将partAggrInfo关联上userInfo
			//然后再直接将返回的tuple的key设为sessionId
			//最后的数据格式还是 <sessionId,partAggrInfo>
			
			//聚合数据拼接:key=value|key=value
			String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionId+"|"
					+Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeyWords+"|"
					+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds;
			
			return new Tuple2<Long, String>(userid,partAggrInfo);
		});
		
		//查询所有用户数据，并映射成Row格式
		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		
		JavaPairRDD<Long, Row> userId2InfoRDD = userInfoRDD.mapToPair(row -> new Tuple2<Long, Row>(row.getLong(0), row));
		
		//将session粒度聚合数据，与用户信息进行join
		JavaPairRDD<Long, Tuple2<String, Row>> userId2FillInfoRDD = sessionId2PartAggrInfoRDD.join(userId2InfoRDD);
		
		//对join起来的数据进行拼接，并返回<sessionId,fullAggrInfo>数据
		JavaPairRDD<String, String> sessionId2FullAggrInfoRDD = userId2FillInfoRDD.mapToPair(tuple2 -> {
			String partAggrInfo = tuple2._2._1;
			Row userInfoRow = tuple2._2._2;
			
			String sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
			
			//获取到用户相关数据
			Integer age = userInfoRow.getInt(3);
			String professional = userInfoRow.getString(4);
			String city = userInfoRow.getString(5);
			String sex = userInfoRow.getString(6);
			
			String fullAggrInfo = partAggrInfo + "|"
					+Constants.FIELD_AGE+"="+age+"|"
					+Constants.FIELD_PROFESSIONAL+"="+professional+"|"
					+Constants.FIELD_CITY+"="+city+"|"
					+Constants.FIELD_SEX+"="+sex;
			
			return new Tuple2<String, String>(sessionId, fullAggrInfo);
		});
		
		
		return null;
	}

	/**
	 * 过滤session数据
	 * @param sessionid2AggrInfoRDD
	 * @return
	 */
	private static JavaPairRDD<String, String> filterSession(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam){
		
		//为了使用后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
		//便于后续性能优化
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");
		
		if (_parameter.endsWith("\\|")) {
			_parameter.substring(0, _parameter.length()-1);
		}
		
		final String parameter = _parameter;
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(tuple -> {
			//先从tuple中获取聚合数据
			String aggrInfo = tuple._2;
			//接着一次按照筛选条件进行过滤
			//按照年龄范围进行过滤(startAge,endAge)
			if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
				return false;
			}
			
			//按照职业范围进行过滤(professionals)
			//互联网，IT，软件
			if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter, Constants.PARAM_PROFESSIONALS)) {
				return false;
			}
			
			//按照城市范围过滤(cities)
			//北京，上海，广州，深圳
			if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
				return false;
			}
			
			//按照性别筛选(sex)
			//男，女
			if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
				return false;
			}
			
			//按照搜索词进行过滤
			if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)) {
				return false;
			}
			
			//按照品类进行筛选
			if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)) {
				return false;
			}
			return true;
		});
		
		return filteredSessionid2AggrInfoRDD;
	}
}
