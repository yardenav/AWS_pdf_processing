package com.amazonaws.samples;


import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Stack;
import java.util.UUID;

import org.apache.commons.io.FileUtils;

import com.amazonaws.services.sqs.model.Message;



public class Task {
	private String bucketName;
	private String fileKey;  
	private int workersRatio; 
	private String taskID;
	private S3Obj inputData;
	private SQSMaster sqsMaster;
	private EC2Master ec2Master;
	private int taskSize;
	private int jobsDone;
	
	// HTML related vars
	private String outputBody;
//	public Task(String taskID, String bucketName, String fileKey, String workRatio, MyS3Object object, boolean isTerminate) {
//		this.bucketName = bucketName;
//		this.fileKey = fileKey;	
//		this.workRatio = workRatio;
//		this.isTerminate = isTerminate;
//		this.taskID = taskID;
//		this.object = object;
//		this.taskSize = 0;
//	}
	//TODO deal with the temination proccess.
	public Task() {
		this.bucketName = null;
		this.fileKey = null;
	}

	public Task(Message message, SQSMaster sqsClient, EC2Master ec2Master) {
		bucketName = message.getMessageAttributes().get("BucketName").getStringValue();
		fileKey = message.getMessageAttributes().get("FileKey").getStringValue();
		workersRatio = Integer.parseInt(message.getMessageAttributes().get("WorkRatio").getStringValue());
		taskID = message.getMessageAttributes().get("TaskID").getStringValue();
		inputData = new S3Obj(bucketName, fileKey); // Downloads the file from s3 bucket.
		taskSize = inputData.countNumOfLines();
		outputBody = "";
		this.sqsMaster = sqsClient;
		this.ec2Master = ec2Master;
		publishTaskInSQS();
		createWorkerInstances();
	}

	// sends an SQS message per every PDF url entry, over the JobsQueue. the messages are delivered to the workers.
	private void publishTaskInSQS() {
		
		System.out.println("Sending each PDF line as an SQS message.");
		
		String line = inputData.read();
		while (line != null){
			//TODO check that the split work like expected maybe its tabs ("\t").
			int to = line.indexOf("To", 0);
			line = line.substring(to);
			
			Stack<String[]> attr = createPdfAttributes(line.split("\\s+"));
			sqsMaster.sendSQSMessage("JobsQueue", "singlePdfTask", attr);
			line = inputData.read();
		}
		
	}
	
	private Stack<String[]> createPdfAttributes(String[] splitted){
		String[] workType = {"WorkType",splitted[0]};
		String[] url = {"URL",splitted[1]};
		String[] taskId = {"TaskID", taskID}; // the taskID received from the local user.
		String[] pdfJobID = {"PdfJobID", UUID.randomUUID().toString()}; // ID for a specific pdf job.
		String[] bucket = {"BucketName",bucketName};
				

		Stack<String[]> attr = new Stack<String[]>();
		attr.push(workType);
		attr.push(url);
		attr.push(taskId);
		attr.push(pdfJobID);
		attr.push(bucket);

		
		
		return attr;
	}
	
	private void createWorkerInstances() {
		int currentWorkers = ec2Master.getNumOfInstances();
		double totalJobs = (sqsMaster.numOfMessagesinQueue("JobsQueue"));
		int workersNeeded = (int)Math.ceil((totalJobs / workersRatio));
		
		int newWorkersAmount = workersNeeded - currentWorkers; 
		if (newWorkersAmount > 0){
			try {
				ec2Master.createInstance(newWorkersAmount);
				System.out.println("Created " + newWorkersAmount+ " new workers.");				
			} catch (Exception e) {
				System.out.println("Couldn't create EC2 instances");
				e.printStackTrace();
			}
		}
		else 
			System.out.println("Didn't Created new workers, there are enough");
	}
	
	public void handlePdfJobDone(Message message) {
		String originalURL = message.getMessageAttributes().get("OriginalURL").getStringValue();
		String resultURL = message.getMessageAttributes().get("ResultURL").getStringValue();
		String workType = message.getMessageAttributes().get("WorkType").getStringValue();
		String totalResult = workType + ": " + originalURL + ">> " + resultURL + "<br/>";
		outputBody = outputBody + totalResult;
		
		
		if (this.decreaseJobsDone()){ //enters if all pdf jobs are done
			System.out.println("All PDF jobs are completed. sending results to local.");
			createHTMLFromOutput();
			uploadToS3();
			String[] bucket = {"BucketName",this.bucketName};
			String[] fileKey = {"FileKey","result-"+this.taskID}; // the taskID received from the local user.
					

			Stack<String[]> attr = new Stack<String[]>();
			attr.push(bucket);
			attr.push(fileKey);
			this.sqsMaster.sendSQSMessage("ResultsQueue", "task done", attr);
		}
		
	}
	
	private void createHTMLFromOutput(){
		File htmlTemplateFile = new File("./htmlTemplate.html");
		
		try {
			String htmlString = FileUtils.readFileToString(htmlTemplateFile,Charset.forName("UTF-8"));	
			String title = "Task Output for task " + taskID;
			htmlString = htmlString.replace("$title", title);
			htmlString = htmlString.replace("$body", this.outputBody);
			File newHtmlFile = new File("./tempOutput.html");
			FileUtils.writeStringToFile(newHtmlFile, htmlString,Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		

	}
	
	private void uploadToS3() {
		S3Obj obj = new S3Obj(this.bucketName, "result-"+this.taskID,"./tempOutput.html");
	}
	
	
	public String getBucketName() {
		return this.bucketName;
	}
	
	// decreases the indicator jobsDone. returns true if it was the last one.
	public boolean decreaseJobsDone() {
		this.jobsDone++;
		if (isTaskFinished()) {
			return true;
		}
		return false;
	}
	
	public boolean isTaskFinished() {
		return ((this.taskSize - this.jobsDone) <= 0);
	}
	

	public String getKey() {
		return this.fileKey;
	}
	
	public int getWorkRatio() {
		return this.workersRatio;
	}
	
	public boolean isValidTask() {
		return bucketName != null && fileKey != null;
	}

	
//	public boolean isTerminate() {
//		return this.isTerminate;
//	}
	
	public String getTaskID() {
		return this.taskID;
	}
	
//	public void setTaskSize(int taskSize) {
//		this.taskSize = taskSize;
//	}
	
//	public int getTaskSize() {
//		return taskSize; 
//	}
	
	public S3Obj getInputObject() {
		return inputData;
	}
}
