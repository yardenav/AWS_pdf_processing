package com.amazonaws.samples;

import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

import com.amazonaws.services.sqs.model.Message;

public class TasksMaster {
	
	private HashMap<String,Task> tasksMap;
	
	public TasksMaster() {
		tasksMap = new HashMap<String,Task>();
	}
	
	public void addTask(Message message,SQSMaster sqsMaster, EC2Master ec2Master){
		Task task = new Task(message,sqsMaster,ec2Master);
		tasksMap.put(task.getTaskID(), task);
	}
	
	public Task getTask(String id){
		return tasksMap.get(id);
	}
	
	public void handlePdfJobDone(Message message) {
		String taskId = message.getMessageAttributes().get("TaskID").getStringValue();
		if (this.tasksMap.containsKey(taskId)) {
			getTask(taskId).handlePdfJobDone(message);
		}
		
	}
	
	public boolean areAllTasksDone(){
		for (Task value : tasksMap.values()) {
			if (!value.isTaskFinished())
				return false;
		}
		return true;
	}


}
