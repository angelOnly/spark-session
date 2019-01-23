package cn.spark.analysis.session.spark;

import org.apache.spark.AccumulatorParam;

import cn.spark.analysis.constants.Constants;
import cn.spark.analysis.util.StringUtils;

/**
 * Session聚合统计Accumulator
 * 
 * 使用自定义的一些数据格式，比如String，自定义model，自定义类(必须可序列化)
 * 然后可以基于这种特殊的数据格式，实现自己复杂的分布式计算逻辑
 * 各个task，分布式运行，可以根据你的需求，task给Accumulator传入不同的值
 * 根据不同的值去做复杂的逻辑
 * @author dali
 *
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String>{

	/**
	 * addInPlace和addAccumulator可以理解为是一样的
	 * v1：初始化的那个连接串
	 * v2：在遍历Session时，判断某个Session对应的区间
	 * 实现功能：在v1中找到v2对应的value，累加1，再更新会连接串
	 */
	@Override
	public String addInPlace(String v1, String v2) {
		return add(v1, v2);
	}

	/**
	 * 数据初始化，返回初始化中所有范围区间的数量，都是0
	 * 各个范围统计数量的拼接格式 key=value|key=value
	 */
	@Override
	public String zero(String v) {
		return Constants.SESSION_COUNT + "=0|"
				+ Constants.TIME_PERIOD_1s_3s + "=0|"
				+ Constants.TIME_PERIOD_4s_6s + "=0|"
				+ Constants.TIME_PERIOD_7s_9s + "=0|"
				+ Constants.TIME_PERIOD_10s_30s + "=0|"
				+ Constants.TIME_PERIOD_30s_60s + "=0|"
				+ Constants.TIME_PERIOD_1m_3m + "=0|"
				+ Constants.TIME_PERIOD_3m_10m + "=0|"
				+ Constants.TIME_PERIOD_10m_30m + "=0|"
				+ Constants.TIME_PERIOD_30m + "=0|"
				+ Constants.STEP_PERIOD_1_3 + "=0|"
				+ Constants.STEP_PERIOD_4_6 + "=0|"
				+ Constants.STEP_PERIOD_7_9 + "=0|"
				+ Constants.STEP_PERIOD_10_30 + "=0|"
				+ Constants.STEP_PERIOD_30_60 + "=0|"
				+ Constants.STEP_PERIOD_60 + "=0";
	}

	@Override
	public String addAccumulator(String v1, String v2) {
		return add(v1, v2);
	}

	/**
	 * 实现Session统计计算逻辑
	 * @param v1 连接串
	 * @param v2 范围区间
	 * @return 更新后的连接串
	 */
	public String add(String v1, String v2) {
		//校验：v1为空，则返回v2
		if (StringUtils.isEmpty(v1)) {
			return v2;
		}
		
		//使用StringUtils，从v1中提取v2对应的值，并累加1
		String oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2);
		
		if (oldValue != null) {
			//将范围区间原有的值，累加1
			int newValue = Integer.valueOf(oldValue) + 1;
			//使用StringUtils工具类将v1中，v2对应的值，累加1
			return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue));
		}
		
		return v1;
	}
}
