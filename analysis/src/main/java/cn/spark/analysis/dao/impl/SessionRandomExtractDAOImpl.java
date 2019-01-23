package cn.spark.analysis.dao.impl;

import cn.spark.analysis.bean.SessionRandomExtract;
import cn.spark.analysis.dao.ISessionRandomExtractDAO;
import cn.spark.analysis.jdbc.JDBCHelper;

public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO{

	@Override
	public void insert(SessionRandomExtract sessionRandomExtract) {
		String sql = "insert into session_random_extract values(?,?,?,?,?)";
		Object[] params = new Object[]{
				sessionRandomExtract.taskId,
				sessionRandomExtract.sessionId,
				sessionRandomExtract.startTime,
				sessionRandomExtract.searchKeyWords,
				sessionRandomExtract.clickCategoryIds
		};
		
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
}
