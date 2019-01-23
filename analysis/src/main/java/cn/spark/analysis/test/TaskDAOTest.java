package cn.spark.analysis.test;

import cn.spark.analysis.bean.Task;
import cn.spark.analysis.dao.ITaskDAO;
import cn.spark.analysis.dao.impl.DaoFactory;

public class TaskDAOTest {

	public static void main(String[] args) {
		
		ITaskDAO taskDAO = DaoFactory.getTaskDAO();
		Task task = taskDAO.findTaskById(1);
		System.out.println(task.name + "-"+task.taskType);
	}

}
