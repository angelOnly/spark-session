package cn.spark.analysis.dao;

import cn.spark.analysis.bean.Task;

/**
 * 任务管理DAO
 * @author dali
 *
 */
public interface ITaskDAO {

	//根据主键查任务
	Task findTaskById(long taskId);
}

