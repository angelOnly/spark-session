package cn.spark.analysis.dao;

import cn.spark.analysis.bean.SessionAggrStat;

/**
 * session聚合统计模块DAO接口
 */
public interface ISessionAggrStatDAO {
	
	void insert (SessionAggrStat sessionAggrStat);

}
