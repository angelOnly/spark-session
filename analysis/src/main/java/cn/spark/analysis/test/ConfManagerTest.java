package cn.spark.analysis.test;

import cn.spark.analysis.conf.ConfManager;

public class ConfManagerTest {

	public static void main(String[] args) {

		String testKey1 = ConfManager.getProperty("testKey1");
		String testKey2 = ConfManager.getProperty("testKey2");
		System.out.println(testKey1 +"--" +testKey2);
	}

}
