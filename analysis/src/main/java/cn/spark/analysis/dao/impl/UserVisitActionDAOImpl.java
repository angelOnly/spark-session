package cn.spark.analysis.dao.impl;

import cn.spark.analysis.bean.UserVisiteAction;
import cn.spark.analysis.dao.IUserVisitActionDAO;
import cn.spark.analysis.jdbc.JDBCHelper;

public class UserVisitActionDAOImpl implements IUserVisitActionDAO{

	@Override
	public void insert(UserVisiteAction userVisiteAction) {
		String sql = "insert into user_visit_action value(?,?,?,?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[] {
				userVisiteAction.date,
				userVisiteAction.userId,
				userVisiteAction.sessionId,
				userVisiteAction.pageId,
				userVisiteAction.actionTime,
				userVisiteAction.searchKeyWord,
				userVisiteAction.clickCategoryId,
				userVisiteAction.clickProductId,
				userVisiteAction.orderCategoryIds,
				userVisiteAction.orderProductIds,
				userVisiteAction.payCategoryIds,
				userVisiteAction.payProductIds};
		
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}
	
}
