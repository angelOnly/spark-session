package cn.spark.analysis.dao.impl;

import cn.spark.analysis.bean.UserInfo;
import cn.spark.analysis.dao.IUserInfoDAO;
import cn.spark.analysis.jdbc.JDBCHelper;

public class UserInfoDAOImpl implements IUserInfoDAO{

	@Override
	public void insert(UserInfo userInfo) {
		String sql = "insert into user_info value(?,?,?,?,?,?,?)";
		Object[] params = new Object[] {
				userInfo.userId,
				userInfo.userName,
				userInfo.name,
				userInfo.age,
				userInfo.professional,
				userInfo.city,
				userInfo.sex};
		
		JDBCHelper.getInstance().executeUpdate(sql, params);
	}

}
