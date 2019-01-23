package cn.spark.analysis.dao.impl;


import cn.spark.analysis.dao.ISessionAggrStatDAO;
import cn.spark.analysis.dao.ISessionDetailDAO;
import cn.spark.analysis.dao.ISessionRandomExtractDAO;
import cn.spark.analysis.dao.ITaskDAO;

public class DaoFactory {
	
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
}
