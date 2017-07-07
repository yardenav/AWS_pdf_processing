package com.amazonaws.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;



public class SQSMaster {

	//private HashMap<String, AmazonSQS> sqsMap = new HashMap<String, AmazonSQS>();
	private AmazonSQS sqsClient;
	
	public SQSMaster(AWSCredentials credentials) {
		//this.sqsClient = AmazonSQSClient(credentials);
		//Region usEast1 = Region.getRegion(Regions.US_EAST_1);
		this.sqsClient = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1).withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        
		//sqsClient.setRegion(usEast1);
	}


	
	/*
	public AmazonSQS getSqsObject(String sqsName) {
		return sqsMap.get(sqsName);
	}*/
	
	public String getQueueUrl(String Name) {
		GetQueueUrlRequest request = new GetQueueUrlRequest().withQueueName(Name);
		return sqsClient.getQueueUrl(request).getQueueUrl();
		
        //return sqsMap.get(Name).createQueue(createTaskQueueRequest).getQueueUrl();
	}
	
	public List<Message> getSQSMessage(String Name) {
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(getQueueUrl(Name));
        receiveMessageRequest.setMaxNumberOfMessages(1);
        //List<Message> messages = sqsClient.rereceiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
        return messages;  
	}
	
	public synchronized void deleteMessage(Message message, String sqsName) {
		// Delete a message
        String messageReceiptHandle = message.getReceiptHandle();
        sqsClient.deleteMessage(getQueueUrl(sqsName), messageReceiptHandle);
	}
	
	// Sends a message to queueName.
	public synchronized void sendSQSMessage(String queueName, String message){
		SendMessageRequest sendMessageRequest = new SendMessageRequest(getQueueUrl(queueName), message);
		sqsClient.sendMessage(sendMessageRequest);			
	}
	
	// Sends the messageBody to the queue with queueName and with the attributes listed on attr as [key,value].
	public synchronized void sendSQSMessage(String queueName, String messageBody, Stack<String[]> attr) {
		SendMessageRequest sendMessageRequest = new SendMessageRequest(getQueueUrl(queueName), messageBody);
		while (!attr.isEmpty()) {
			String[] kv = attr.pop();
			sendMessageRequest.addMessageAttributesEntry(kv[0], new MessageAttributeValue().withDataType("String").withStringValue(kv[1]));
		}
		sqsClient.sendMessage(sendMessageRequest);
	
	}
	
	public int numOfMessagesinQueue(String queueName){
		List<String> list = new ArrayList<String>();
		list.add("ApproximateNumberOfMessages");
		GetQueueAttributesResult result = sqsClient.getQueueAttributes(getQueueUrl(queueName), list);
        Map<String, String> sqsAttributeMap = result.getAttributes();
        return Integer.parseInt(sqsAttributeMap.get("ApproximateNumberOfMessages"));
	}
	
	public void createQueue(String queueName) {
        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        sqsClient.createQueue(createQueueRequest);

	}
	
	public void closeQueue(String queueName) {
		this.sqsClient.deleteQueue(queueName);
	}
	
}
