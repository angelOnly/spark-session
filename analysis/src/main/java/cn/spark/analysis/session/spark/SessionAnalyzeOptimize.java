package cn.spark.analysis.session.spark;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import com.alibaba.fastjson.JSONObject;

import cn.spark.analysis.bean.SessionAggrStat;
import cn.spark.analysis.bean.SessionDetail;
import cn.spark.analysis.bean.SessionRandomExtract;
import cn.spark.analysis.bean.Task;
import cn.spark.analysis.conf.ConfManager;
import cn.spark.analysis.constants.Constants;
import cn.spark.analysis.dao.ISessionAggrStatDAO;
import cn.spark.analysis.dao.ISessionDetailDAO;
import cn.spark.analysis.dao.ISessionRandomExtractDAO;
import cn.spark.analysis.dao.ITaskDAO;
import cn.spark.analysis.dao.impl.DaoFactory;
import cn.spark.analysis.dao.impl.SessionDetailDAOImpl;
import cn.spark.analysis.test.MockData;
import cn.spark.analysis.util.DateUtils;
import cn.spark.analysis.util.NumberUtils;
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
public class SessionAnalyzeOptimize {

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
		
		System.out.println("taskParam:"+taskParam);
		
		//如果要进行session粒度的数据聚合，首先要从user_visit_action表中，查询出指定日期范围内的行为数据
		//首先要根据用户在创建任务时指定的参数，来进行数据过滤和筛选
		JavaRDD<Row> actionRDD = getActionRDDByDataRange(sqlContext, taskParam);
		JavaPairRDD<String, Row> sessionid2actionRDD = getSessionid2ActionRDD(actionRDD);
		
		
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
		
		/**
		 * =================================================================================
		 *      								重构开始
		 * =================================================================================
		 * 重构，同时进行过滤和统计
		 */
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
		/**
		 * =================================================================================
		 *      								重构结束
		 * =================================================================================
		 */
		//接着就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		JavaPairRDD<String, String> filterdSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionId2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		
		System.out.println("Accumulato value:"+sessionAggrStatAccumulator.value());
		
		//653
		System.out.println("session过滤数据："+filterdSessionid2AggrInfoRDD.count());
		for (Tuple2<String, String> tuple : filterdSessionid2AggrInfoRDD.take(10)) {
			System.out.println(tuple._1+":"+tuple._2);
		}
		
		/**
		 * 特别说明
		 * 我们知道，要将一个功能的session聚合统计数据获取到，就必须是在一个action操作触发job之后才能
		 * 从Accmulator中获取数据，否则是获取不到数据的，因为没有job执行，Accumulator的值为空
		 * 所以，我们在这里，将随机抽取功能的实现代码放在session聚合统计功能的最终计算和写库之前
		 * 因为随机抽取功能中，有一个countByKey算子，是action操作，会触发job
		 */
		
		/**
		 * 每次执行用户访问session分析模块，要抽取100个session
		 * Session随机抽取：按每天的每小时的Session数量占当天总数比例，乘以每天要抽取的Session数量，计算出每个小时要抽取的Session数量
		 * 然后在每天每个小时的Session中，随机抽取出执勤啊计算出来的数量的Session
		 * 
		 * 如：10000个Session，要抽100个Session，在 0s-3s中有2000个Session，占比0.2，按照比例抽取的Session数量就是 100*0.2=20个
		 * 
		 * 对之前计算出的Session粒度集合数据进行映射，将每个Session发生的 yyyy-MM-dd_HH(start_time)作为key，value就是session_id
		 * 对上述数据，使用countByKey算子就可以获取到每天每小时的Session数量
		 * 
		 * （按时间比例随机抽取算法）每天每小时有多少Session，根据这个数量计算出每天每小时的Session占比，以及按照占比需要抽取多少Session，
		 * 可以计算出每小时内，从 0~x 的Session数量之间的范围中获取指定抽取数量个随机数，作为随机抽取索引。
		 * 
		 * 把之前转行后的Session数据(以yyyy-MM-dd_HH作为key)，执行groupByKey算子，然后可以遍历每天每小时的Session，
		 * 遍历时，遇到之前计算出来的要抽取的索引，即将Session抽取出来，写入MySQL
		 */
		
		randomExtractSession(task.taskId, filterdSessionid2AggrInfoRDD, sessionid2actionRDD);
		
		//计算出各个范围内的Session占比，并写入MySQL
		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.taskId);
		
		
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
		
		//获取top10热门品类
		getTop10Category(filterdSessionid2AggrInfoRDD, sessionid2actionRDD);
		
		sc.close();
	}
	
	/**
	 * 获取top10热门品类
	 * @param filterdSessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static void getTop10Category(JavaPairRDD<String, String> filterdSessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		/**
		 * 1. 获取符合条件的session访问的所有品类
		 */
		//获取符合条件的session明细
		JavaPairRDD<String, Row> sessionid2detailRDD = filterdSessionid2AggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(tuple -> {
					return new Tuple2<String, Row>(tuple._1, tuple._2._2);
				});
		
		//获取session访问过的所有品类id
		//访问过指的是：点击过，下单过，支付过的品类
		JavaPairRDD<Long, Long> categoryIdRDD = sessionid2detailRDD.flatMapToPair(tuple -> {
			Row row = tuple._2;
			List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long,Long>>();
			
			Long clickCategoryId = row.getLong(6);
			
			if (clickCategoryId != null) {
				list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
			}
			String orderCategoryIds = row.getString(8);
			if (StringUtils.isNotEmpty(orderCategoryIds)) {
				String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
				for(String orderCategoryId : orderCategoryIdsSplited) {
					list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
				}
			}
			
			String payCategoryIds = row.getString(10);
			if (StringUtils.isNotEmpty(payCategoryIds)) {
				String[] payCategoryIdsSplited = payCategoryIds.split(",");
				for(String payCategoryId : payCategoryIdsSplited) {
					list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
				}
			}
			return list;
		});
		
		/**
		 * 2.计算各品类的点击，下单和支付的次数
		 */
		//访问明细中，其中三种访问行为是：点击，下单，支付
		//所以分别计算各品类点击，下单，支付才次数，先对访问明细数据进行过滤，分别过滤出点击，下单，支付行为，然后通过map，reduceByKey等算子进行计算

		//计算各个品类的点击次数
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);
		
		//计算各个品类的下单次数
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);
		
		//计算各个品类的支付次数
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);
		
	}

	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD.filter(tuple -> tuple._2.getString(10)!=null);
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payActionRDD.flatMapToPair(tuple -> {
			Row row = tuple._2;
			String payCategoryIds = row.getString(10);
			String[] payCategoryIdsSplited = payCategoryIds.split(",");
			
			List<Tuple2<Long,Long>> list = new ArrayList<>();
			for(String payCategoryId: payCategoryIdsSplited) {
				list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
			}
			return list;
		});
		payCategoryId2CountRDD.reduceByKey((v1,v2) -> v1+v2);
		return payCategoryId2CountRDD;
	}

	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD.filter(tuple -> tuple._2.getString(8)!=null);
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderActionRDD.flatMapToPair(tuple -> {
			Row row = tuple._2;
			String orderCategoryIds = row.getString(8);
			String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
			
			List<Tuple2<Long,Long>> list = new ArrayList<>();
			for(String orderCategoryId: orderCategoryIdsSplited) {
				list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
			}
			return list;
		});
		
		orderCategoryId2CountRDD.reduceByKey((v1,v2) -> v1+v2);
		return orderCategoryId2CountRDD;
	}

	public static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD.filter(tuple -> Long.valueOf(tuple._2.getLong(6))!=null);
		
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickActionRDD.mapToPair(tuple -> {
			long clickCategoryId = tuple._2.getLong(6);
			return new Tuple2<Long, Long>(clickCategoryId, 1L);
		});
		
		clickCategoryId2CountRDD.reduceByKey((v1,v2) -> v1+v2);
		return clickCategoryId2CountRDD;
	}

	/**
	 * 随机抽取Session
	 * @param taskId
	 * @param sessionid2AggrInfoRDD
	 * @param sessionid2actionRDD
	 */
	private static void randomExtractSession(long taskId, 
			JavaPairRDD<String, String> sessionid2AggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		/**
		 * 1. 计算出每天每小时Session数量，获取 <yyyy-MM-dd_HH, aggrInfo> 格式RDD
		 */
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD.mapToPair(tuple -> {
			String aggrInfo = tuple._2;
			String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
			String dateHour = DateUtils.getDateHour(startTime);
			return new Tuple2<String, String>(dateHour, aggrInfo);
		});
		//得到每天每小时的Session数量
		Map<String, Object> countMap = time2sessionidRDD.countByKey();
		
		/**
		 * 2.按时间比例随机抽取算法，计算每天每小时要抽取的Session的索引
		 */
		//将 <yyyy-MM-dd_HH,count> 格式的map转成 <yyyy-MM-dd,<HH,count>>的格式
		Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String,Long>>();
		
		for (Map. Entry<String, Object> countEntry : countMap.entrySet()) {
			String dateHour = countEntry.getKey();
			String date = dateHour.split("-")[0];
			String hour = dateHour.split("-")[1];
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));
			
			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if (hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}
			
			hourCountMap.put(hour, count);
		}
		
		//开始计算按时间比例随机抽取算法
		//总共抽取100个Session，先按照天数进行平分
		long extractNumberPerDay = 100 / dateHourCountMap.size();

		//<date, <hour, <sessionid>>>   <date, <hour, (2,3,4,5,6)>>
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
		
		Random random = new Random();
		
		for(Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();
			//计算这一天是Session总数
			long sessionCount = 0L;
			
			for(long hourCount:hourCountMap.values()) {
				sessionCount += hourCount;
			}
			
		
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if (hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}
			
			//遍历每个小时
			for(Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();
				
				//计算每小时Session数量按当天Session的比例，直接乘以每天要抽取的数量，就可以计算出当前小时需要抽取的Session数量
				long hourExtractNumber = (long) ((double)count / (double)sessionCount * extractNumberPerDay);  
				
				if (hourExtractNumber > count) {
					hourExtractNumber = count;
				}
				
				//先获取当前小时的存放随机数的list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}
				
				//生成上述计算出来的数量的随机数
				for(int i=0; i<hourExtractNumber; i++) {
					int extractIdx = random.nextInt((int)count);
					while (extractIndexList.contains(extractIdx)) {
						extractIdx = random.nextInt((int)count);
					}
					extractIndexList.add(extractIdx);
				}
			}
		}
		
		/**
		 * 3.遍历每天每小时的Session，根据随机索引进行抽取
		 */
		//执行groupByKey算子，得到 <dateHour, (session, aggeInfo)> 
		JavaPairRDD<String, Iterable<String>> time2sessionRDD = time2sessionidRDD.groupByKey();
		
		//使用flatMap算子，遍历所有的 <dateHour, (session aggrInfo) 格式数据
		//然后会遍历每天每小时的Session，如果发现某个Session恰巧在我们指定的这天这小时的随机抽取索引上
		//那么抽取该Session，直接写入MySQL的random_extract_session表
		//将抽取出的Session id返回，形成一个新的JavaRDD
		//最后一步，用抽取来的Session id，去join访问的明细数据，写入session_detail表
		JavaPairRDD<String,String> extractSessionsRDD = time2sessionRDD.flatMapToPair(tuple -> {
			List<Tuple2<String, String>> extractSessionList = new ArrayList<>();
			
			String dateHour = tuple._1;
			String date = dateHour.split("-")[0];
			String hour = dateHour.split("-")[1];
			
			//该天该小时要随机抽取的索引
			List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
			
			Iterator<String> iterator = tuple._2.iterator();
			
			ISessionRandomExtractDAO sessionRandomExtractDAO = DaoFactory.getSessionRandomExtractDAO();
			
			int index = 0;
			while (iterator.hasNext()) {
				String sessionAggrInfo = iterator.next();
				if (extractIndexList.contains(index)) {
					String sessionId = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SESSION_ID);;
					//将数据写入MySQL
					SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
					sessionRandomExtract.taskId = taskId;
					sessionRandomExtract.sessionId = sessionId;
					sessionRandomExtract.startTime = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_START_TIME);
					sessionRandomExtract.searchKeyWords = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS);
					sessionRandomExtract.clickCategoryIds = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS);
					
					System.out.println("searchKeyWords:"+StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
					sessionRandomExtractDAO.insert(sessionRandomExtract);
					
					//将session id 加入list
					extractSessionList.add(new Tuple2<String, String>(sessionId, sessionId));
				}
				index++;
			}
			return extractSessionList;
		});
		
		/**
		 * 4.获取抽取出来的明细数据
		 */
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionsRDD.join(sessionid2actionRDD);
		extractSessionDetailRDD.foreach(tuple -> {
			Row row = tuple._2._2;
			SessionDetail sessionDetail = new SessionDetail();
			sessionDetail.taskId = taskId;
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
		});
		
	}

	/**
	 * 获取sessionid2到访问行为数据的映射的RDD
	 * @param actionRDD
	 * @return
	 */
	public static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
		return actionRDD.mapToPair(row -> new Tuple2<String, Row>(row.getString(2), row));
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
		String sql = "select * from user_visit_action";
		
		//where date>='"+startDate+"' and date<='"+endDate+"'
		DataFrame actionDF = sqlContext.sql(sql);
		
		System.out.println("startDate:"+startDate+" endDate:"+endDate+"actionDF:"+actionDF.count());
		
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
			
			/**
			 * =================================================================================
			 *      								重构开始
			 * =================================================================================
			 */
			//Session起始时间
			Date startTime = null;
			Date endTime = null;
			//Session访问步长
			int stepLength = 0;
			/**
			 * =================================================================================
			 *      								重构结束
			 * =================================================================================
			 */
			
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
				if (StringUtils.isNotEmpty(searchKeyWord)) {
					if (!searchKeyWordsBuffer.toString().contains(searchKeyWord)) {
						searchKeyWordsBuffer.append(searchKeyWord+",");
					}
				}
				
				if (clickCategoryId != null) {
					if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
						clickCategoryIdsBuffer.append(clickCategoryId+",");
					}
				}
				
				/**
				 * =================================================================================
				 *      								重构开始
				 * =================================================================================
				 */
				//计算Session开始和结束时间
				Date actionTime = DateUtils.parseTime(row.getString(4));
				if (startTime == null) {
					startTime = actionTime;
				}
				
				if (endTime == null) {
					endTime = actionTime;
				}
				
				if (actionTime.before(startTime)) {
					startTime = actionTime;
				}
				
				if (actionTime.after(endTime)) {
					endTime = actionTime;
				}
				
				//计算访问步长
				stepLength++;
				
				/**
				 * =================================================================================
				 *      								重构结束
				 * =================================================================================
				 */
			}
			
			String searchKeyWords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
			String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
			
			/**
			 * =================================================================================
			 *      								重构开始
			 * =================================================================================
			 */
			//计算Session访问时长，单位：秒
			long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
			
			/**
			 * =================================================================================
			 *      								重构结束
			 * =================================================================================
			 */
			//返回的数据格式是<sessionId,partAggrInfo>，这一步聚合玩之后，还需要将每一行数据，跟对应的用户信息进行聚合
			//问题来了，如果跟用户信息进行聚合，那么key就不应该是sessionId，而是userId
			//才能跟<userId,Row>格式的用户信息进行聚合
			//如果这里直接返回<sessionId,partAggrInfo>,还需要再做一次mapToPair算子
			//将RDD映射成<userId,Row>格式，显得多余
			
			//所以这里直接返回的数据格式为<userId,partAggrInfo>，然后跟用户信息join的时候，将partAggrInfo关联上userInfo
			//然后再直接将返回的tuple的key设为sessionId
			//最后的数据格式还是 <sessionId,partAggrInfo>
			
			/**
			 * =================================================================================
			 *      								重构开始
			 * =================================================================================
			 */
			//聚合数据拼接:key=value|key=value
			String partAggrInfo = Constants.FIELD_SESSION_ID+"="+sessionId+"|"
					+Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeyWords+"|"
					+Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds+"|"
					//重构添加新字段：访问时长和访问步长
					+Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
					+Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"
					+Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime);
			
			/**
			 * =================================================================================
			 *      								重构结束
			 * =================================================================================
			 */
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
		
		
		return sessionId2FullAggrInfoRDD;
	}

	/**
	 * 过滤session数据
	 * @param sessionid2AggrInfoRDD
	 * @return
	 */
	
	
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator){
		
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
		
		System.out.println("parameter:"+parameter);
		
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
			
			@Override
			public Boolean call(Tuple2<String, String> tuple) throws Exception {
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
				
				/**
				 * =================================================================================
				 *      								重构开始
				 * =================================================================================
				 */
				//如果经过了之前的多个过滤条件后，那么程序能走到这里，那么说明该Session是通过了用户指定的筛选条件的，也就是需要保留的Session
				//那么就要对Session的访问时长和访问步长进行统计，根据Session对应的范围进行相应的累加计数
				sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
				
				//计算Session的访问时长和访问步长的范围，并进行相应的累加
				long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
				long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
				
				calculateVisitLength(visitLength);
				calculateStepLength(stepLength);
				
				System.out.println("visitLength:"+visitLength+" stepLength:"+stepLength);
				return true;
			}
			
			/**
			 * 统计访问时长
			 * @param visitLength
			 */
			private void calculateVisitLength(long visitLength) {
				if (visitLength >= 1 && visitLength < 3) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
				} else if (visitLength >= 4 && visitLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
				} else if (visitLength >= 7 && visitLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
				} else if (visitLength >= 10 && visitLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
				} else if (visitLength > 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
				} else if (visitLength > 60 && visitLength <= 180) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
				} else if (visitLength > 30 && visitLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
				} else if (visitLength > 180 && visitLength <= 600) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
				} else if (visitLength > 600 && visitLength <= 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
				} else if (visitLength > 1800) {
					sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
				}
			} 
			
			/**
			 * 统计访问步长
			 * @param stepLength
			 */
			private void calculateStepLength(long stepLength) {
				if (stepLength >= 1 && stepLength < 3) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
				} else if (stepLength >= 4 && stepLength <= 6) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
				} else if (stepLength >= 7 && stepLength <= 9) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
				} else if (stepLength >= 10 && stepLength <= 30) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
				} else if (stepLength > 30 && stepLength <= 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
				} else if (stepLength > 60) {
					sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
				}
			} 
			/**
			 * =================================================================================
			 *      								重构结束
			 * =================================================================================
			 */
		});
		
		return filteredSessionid2AggrInfoRDD;
	}
	
	/*
	 * 计算各session范围占比，并写入MySQL
	 */
	private static void calculateAndPersistAggrStat(String value, long taskid) {
		//从Accumulator统计串中获取值
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.SESSION_COUNT));
		
		long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.TIME_PERIOD_30m));
		
		long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
				value, "\\|", Constants.STEP_PERIOD_60));
		
		System.out.println("1s_3s:"+visit_length_1s_3s +" -- "+session_count+"--"+value+"--"+taskid);
		
		//计算各个访问时长和访问步长的范围
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
				(double)visit_length_1s_3s / (double)session_count, 2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
				(double)visit_length_4s_6s / (double)session_count, 2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
				(double)visit_length_7s_9s / (double)session_count, 2);
		double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
				(double)visit_length_10s_30s / (double)session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
				(double)visit_length_30s_60s / (double)session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
				(double)visit_length_1m_3m / (double)session_count, 2);
		double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
				(double)visit_length_3m_10m / (double)session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_10m_30m / (double)session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble(
				(double)visit_length_30m / (double)session_count, 2);
		
		double step_length_1_3_ratio = NumberUtils.formatDouble(
				(double)step_length_1_3 / (double)session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble(
				(double)step_length_4_6 / (double)session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble(
				(double)step_length_7_9 / (double)session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble(
				(double)step_length_10_30 / (double)session_count, 2);
		double step_length_30_60_ratio = NumberUtils.formatDouble(
				(double)step_length_30_60 / (double)session_count, 2);
		double step_length_60_ratio = NumberUtils.formatDouble(
				(double)step_length_60 / (double)session_count, 2);
		
		//将访问结果封装成Domain对象
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
		
		//调用对应的DAO插入统计结果
		ISessionAggrStatDAO sessionAggrStatDAO = DaoFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);
	}
}
