package cn.spark.analysis.bean;

import java.io.Serializable;

public class Task implements Serializable{

	public static final long serialVersionUID = -9205169808581154064L;
	
	public long taskId;
	public String name;
	public String createTime;
	public String startTime;
	public String finishTime;
	public String taskType;
	public String taskStatus;
	public String taskParam;
	
	public Task() {}
	
	public Task(long taskId, String name, String createTime, String startTime, String finishTime, String taskType,
			String taskStatus, String taskParam) {
		super();
		this.taskId = taskId;
		this.name = name;
		this.createTime = createTime;
		this.startTime = startTime;
		this.finishTime = finishTime;
		this.taskType = taskType;
		this.taskStatus = taskStatus;
		this.taskParam = taskParam;
	}
}
