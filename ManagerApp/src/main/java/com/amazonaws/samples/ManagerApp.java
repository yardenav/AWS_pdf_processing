package com.amazonaws.samples;

import java.util.List;


import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.model.Message;

public class ManagerApp {
	
	private static SQSMaster sqsMaster;
	private static EC2Master ec2Master;
	private static TasksMaster tasksMaster;
	
	public static void main(String[] args) throws Exception { 	
        AWSCredentials credentials = null;
    	boolean isTerminated = false;
    	
        try {
            credentials = new ProfileCredentialsProvider("./credentials","default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (~/.aws/credentials), and is in valid format.",
                    e);
        }
    
        sqsMaster = new SQSMaster(credentials);
        ec2Master = new EC2Master(credentials);
        tasksMaster = new TasksMaster();
        
        sqsMaster.createQueue("JobsQueue");
        sqsMaster.createQueue("pdfDoneQueue");

//        
//        List<Message> taskMessages = sqsMaster.getSQSMessage("TaskQueue");
//        Message message = taskMessages.get(0);
//        for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
//            System.out.println("  Attribute");
//            System.out.println("    Name:  " + entry.getKey());
//            System.out.println("    Value: " + entry.getValue());
//        }
//      
        
        try {     
        	int i = 0;
            while(true) {
            	System.out.println("Pollng for messages........" + i);
            	List<Message> messages = sqsMaster.getSQSMessage("TaskQueue");
            	System.out.println(messages);////////////////////////////
            	if (!messages.isEmpty() && !isTerminated) {
            		Message message = messages.get(0);
            		
            		// If it's a TASK
	            	if (message.getBody().equals("task")) {	
	            		tasksMaster.addTask(message,sqsMaster,ec2Master);
	            		sqsMaster.deleteMessage(message, "TaskQueue");
	            		System.out.println("Task arrived and Message deleted");	            		
	            	} 
	            	// If it's a termination command
	            	else if (message.getBody().equals("terminate")) {
	            		System.out.println("#Starting to Terminate the manager..");
	        		    System.out.println("#Waiting for previous tasks to complete");
	            		isTerminated = true;

	        		    while (!tasksMaster.areAllTasksDone()) {
	        		    	checkForPdfDoneMessages();
	        		    }
	        		    ec2Master.terminateWorkerInstances();	    
	            		sqsMaster.closeQueue("JobsQueue");
	            		sqsMaster.closeQueue("pdfDoneQueue");
	            		sqsMaster.closeQueue("TaskQueue");
	            		sqsMaster.sendSQSMessage("ResultsQueue", "terminated");


	            	}    	 
            	}
            	
            	// Handling pdf job is done from worker
            	checkForPdfDoneMessages();
            	Thread.sleep(1000); //TODO is it too much?
            	i++;
         }
            	       
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
        

    }
	
	private static void checkForPdfDoneMessages(){
		System.out.println("Cheking for pdfDoneJ");
    	List<Message> pdfDoneMessages = sqsMaster.getSQSMessage("pdfDoneQueue");
    	if (!pdfDoneMessages.isEmpty()) {
    		Message pdfDoneMessage = pdfDoneMessages.get(0);
    		System.out.println("MESSAGE RECEIVED :" + pdfDoneMessage.getBody());

        	if (pdfDoneMessage.getBody().equals("PdfDoneTask")) {	
        		//decrease the number of pdf jobs done.
        		System.out.println("New PDF-Job-Done Arrived");
        		tasksMaster.handlePdfJobDone(pdfDoneMessage);
        		
        		
        		
        		sqsMaster.deleteMessage(pdfDoneMessage, "pdfDoneQueue");
        	} 	
    	}
		
	    
	}
	

	

	
}
