package cn.spark.analysis.dao.impl;

import java.sql.ResultSet;

import cn.spark.analysis.bean.Task;
import cn.spark.analysis.dao.ITaskDAO;
import cn.spark.analysis.jdbc.JDBCHelper;
import cn.spark.analysis.jdbc.JDBCHelper.QueryCallback;

/**
 * TaskDAO实现类
 * @author dali
 * mysql> CREATE TABLE `task` (
    ->   `id` INTEGER(11) NOT NULL AUTO_INCREMENT,
    ->   `name` VARCHAR(255) NULL DEFAULT NULL,
    ->   `create_time` VARCHAR(255) NULL DEFAULT NULL,
    ->   `start_time` VARCHAR(255) NULL DEFAULT NULL,
    ->   `finish_time` VARCHAR(255) NULL DEFAULT NULL,
    ->   `task_type` VARCHAR(255) NULL DEFAULT NULL,
    ->   `task_status` VARCHAR(255) NULL DEFAULT NULL,
    ->   `task_param` MEDIUMTEXT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`id`)
    -> )ENGINE=InnoDB DEFAULT CHARSET=utf8;
	Query OK, 0 rows affected, 1 warning (0.09 sec)
 */
public class TaskDAOImpl implements ITaskDAO{

	public Task findTaskById(long taskId) {
		final Task task = new Task();
		String sql = "select * from task where id=?";
		Object[] params = new Object[]{taskId};
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		
		jdbcHelper.executeQuery(sql, params, new QueryCallback() {
			public void process(ResultSet rs) throws Exception {
				if (rs.next()) {
					task.taskId = rs.getLong("id");
					task.name = rs.getString("name");
					task.createTime = rs.getString("create_time");
					task.startTime = rs.getString("start_time");
					task.finishTime = rs.getString("finish_time");
					task.taskType = rs.getString("task_type");
					task.taskStatus = rs.getString("task_status");
					task.taskParam = rs.getString("task_param");
				}
			}
		});
		return task;
	}
}
