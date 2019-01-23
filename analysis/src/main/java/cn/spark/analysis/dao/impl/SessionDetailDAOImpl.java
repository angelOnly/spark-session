package cn.spark.analysis.dao.impl;

import cn.spark.analysis.bean.SessionDetail;
import cn.spark.analysis.dao.ISessionDetailDAO;
import cn.spark.analysis.jdbc.JDBCHelper;

public class SessionDetailDAOImpl implements ISessionDetailDAO{

	@Override
	public void insert(SessionDetail sessionDetail) {
		String sql = "insert into session_detail value(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] {
				sessionDetail.taskId,
				sessionDetail.userId,
				sessionDetail.sessionId,
				sessionDetail.pageId,
				sessionDetail.actionTime,
				sessionDetail.searchKeyWord,
				sessionDetail.clickCategoryId,
				sessionDetail.clickProductId,
				sessionDetail.orderCategoryIds,
				sessionDetail.orderProductIds,
				sessionDetail.payCategoryIds,
				sessionDetail.payProductIds};
		
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
	
}
