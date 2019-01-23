package cn.spark.analysis.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class FastJsonTest {

	public static void main(String[] args) {
		
		String json = "[{'学生':'张三','班级':'一班','年级':'大一','科目':'高数','成绩':90},"
				+ "{'学生':'李四','班级':'二班','年级':'大一','科目':'高数','成绩':80}]";
		
		JSONArray jsonArray = JSONArray.parseArray(json);
		JSONObject object = jsonArray.getJSONObject(0);
		System.out.println(object.getString("学生"));
	}

}
