package com.amazonaws.samples;

/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import org.apache.commons.codec.binary.Base64;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;



public class LocalApp {
	public static void main(String[] args) throws IOException {
		if (args.length > 3) System.out.println("program launched with "+ args[3] + "argument");
		String taskID = UUID.randomUUID().toString(); //This taskID represents the LocalApp running instance.
		
		
        //Attempting to acquire credentials.
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Credentials could not load from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct " +
                    "location and is in valid format.",
                    e);
        }
        //Credentials acquired.
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        //Acquire S3 and SQS from AWS using the region usEast1.
        AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion(Regions.US_EAST_1)
				.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        AmazonS3 s3 = new AmazonS3Client(credentials); 
        s3.setRegion(usEast1);
        String bucketName = "pdf-files-bucket-idan-and-yarden";
        String fileKey = "Input";
        //Done.
        ArrayList<String> instanceIDs = CreateManagerEC2Node(); //Initiates a manager node on the EC2 AWS and runs it if it is not already running.
        
        //Creates a task queue
		CreateQueueRequest createTaskQueueRequest = new CreateQueueRequest("TaskQueue");
		String TaskQueueUrl = sqs.createQueue(createTaskQueueRequest).getQueueUrl();
		
        try {
			s3.createBucket(bucketName); //Creates an S3 bucket.

			
			//Creates a result queue
			CreateQueueRequest createResultQueueRequest = new CreateQueueRequest("ResultsQueue");
			String ResultQueueUrl = sqs.createQueue(createResultQueueRequest).getQueueUrl();

			UploadFileToS3(s3, bucketName, fileKey, "./input.txt"); //Uploads a file to the bucket
			
			//Attempts to send a new message on TaskQueue.
			SendMessageRequest sendMessageRequest = new SendMessageRequest(TaskQueueUrl, "task");
			Map<String, MessageAttributeValue> sendMessageAttributes =
			        new HashMap<String, MessageAttributeValue>();
			sendMessageAttributes.put("BucketName", new MessageAttributeValue()
			        .withDataType("String")
			        .withStringValue(bucketName));
			sendMessageAttributes.put("FileKey", new MessageAttributeValue()
			        .withDataType("String")
			        .withStringValue(fileKey));
			sendMessageAttributes.put("WorkRatio", new MessageAttributeValue()
			        .withDataType("String")
			        .withStringValue(args[2]));
			sendMessageAttributes.put("TaskID", new MessageAttributeValue()
			        .withDataType("String")
			        .withStringValue(taskID));
			sendMessageRequest.setMessageAttributes(sendMessageAttributes);
			sqs.sendMessage(sendMessageRequest);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (SdkClientException e) {
			e.printStackTrace();
		}
        
        Boolean finished = false;
        String resultBucketName, resultFileKey;
        while(!finished) {

            try {
                	TimeUnit.SECONDS.sleep(5);
                } 
            catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Polling ResultsQueue...");

            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest("ResultsQueue");
            List<Message> messages = sqs.receiveMessage(receiveMessageRequest.
            		withMessageAttributeNames("All")).getMessages();
            if (!messages.isEmpty()) {
            	Message message = messages.get(0);
                resultBucketName = message.getMessageAttributes().get("BucketName").
                		getStringValue();
                resultFileKey = message.getMessageAttributes().get("FileKey").
                		getStringValue();
                createFileFromInputStream(s3.getObject(new GetObjectRequest(resultBucketName, resultFileKey))
                		.getObjectContent(), args[1]);
                CreateQueueRequest createResultQueueRequest = new CreateQueueRequest("ResultsQueue");
                String ResultQueueUrl = sqs.createQueue(createResultQueueRequest).getQueueUrl();
                sqs.deleteMessage(ResultQueueUrl, message.getReceiptHandle());
                System.out.println("Done.\n");
                if (args.length>3 && args[3].equals("terminate")) {
        			SendMessageRequest sendMessageRequest = new SendMessageRequest(TaskQueueUrl, "terminate");
        			sqs.sendMessage(sendMessageRequest);
        			boolean managerRunning=true;
        			while (managerRunning) {
        				try {
                        	TimeUnit.SECONDS.sleep(5);
                        } 
	                    catch(InterruptedException ex) {
	                        Thread.currentThread().interrupt();
	                    }
	                    System.out.println("Waiting For terminated message");
	
	                    receiveMessageRequest = new ReceiveMessageRequest("ResultsQueue");
	                    messages = sqs.receiveMessage(receiveMessageRequest.
	                    		withMessageAttributeNames("All")).getMessages();
	                    if (!messages.isEmpty() && messages.get(0).getBody().equals("terminated") ) {
	                    	AmazonEC2 ec2 = createEC2Client();
	                    	ec2.terminateInstances(new TerminateInstancesRequest(instanceIDs));
	                    	sqs.deleteQueue(sqs.getQueueUrl("ResultsQueue").getQueueUrl());
	                    	managerRunning = false;
	                    }
	
        			}
                }
                finished=true;
            }
            
        }        

	}
	
	private static AmazonEC2 createEC2Client() {
		
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Credentials could not load from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct " +
                    "location and is in valid format.",
                    e);
        }
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withEndpointConfiguration(
        		new EndpointConfiguration("ec2.us-east-1.amazonaws.com","us-east-1"))
        		.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        return ec2;
	}

	//returns an array with the manager instanceId, for termination purposes
	static ArrayList<String> CreateManagerEC2Node() throws IOException {
		
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Credentials could not load from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct " +
                    "location and is in valid format.",
                    e);
        }
        AmazonEC2 ec2 = AmazonEC2ClientBuilder.standard().withEndpointConfiguration(
        		new EndpointConfiguration("ec2.us-east-1.amazonaws.com","us-east-1"))
        		.withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
        
        if (!isManagerRunning(ec2)) {
        	 RunInstancesRequest request = new RunInstancesRequest("ami-c58c1dd3", 1, 1);
             request.setInstanceType(InstanceType.T2Micro.toString());
             request.setUserData(getManagerAppScript(credentials));
             RunInstancesResult res = ec2.runInstances(request);
             
             String instanceID = res.getReservation().getInstances().get(0).getInstanceId();
             ArrayList<String> instanceIDs = new ArrayList<String>();
             ArrayList<Tag> tags = new ArrayList<Tag>();
             Tag tag = new Tag(instanceID, "manager");
             tags.add(tag);
             instanceIDs.add(instanceID);
             ec2.createTags(new CreateTagsRequest(instanceIDs, tags));
             return instanceIDs;

        }
        else {
        	DescribeInstancesRequest request = new DescribeInstancesRequest();
        	List<Reservation> result = ec2.describeInstances(request).getReservations();
    		ArrayList<String> instanceIDs = new ArrayList<String>();
        	for (Reservation reservation : result){
        		for (Instance instance : reservation.getInstances()){
        			List<Tag> tags = instance.getTags();
        			if (!tags.isEmpty() && tags.get(0).equals("manager"))
        				instanceIDs.add(instance.getInstanceId());
        			System.out.println("Manager is already running...***InstanceIDs size is :" + instanceIDs.size());
        			return instanceIDs;
        		}
        	}
        }
        System.out.println("Couldn't find manager instance, it wasn't created");
        return null;
	}
	/*
	 * Uploads a file from the path fileName to the bucket bucketName in s3 under the name 
	 *fileKey unless a file in the bucket with the name fileKey exists, in which case it 
	 *replaces the old file.
	*/
	static void UploadFileToS3(AmazonS3 s3, String bucketName, String fileKey, String fileName) {
        File file = new File(fileName);
        s3.putObject(new PutObjectRequest(bucketName, fileKey, file));
	}
    private static boolean isManagerRunning(AmazonEC2 ec2) {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        List<Reservation> result = ec2.describeInstances(request).getReservations();
        for (Reservation reservation : result)
            for (Instance instance : reservation.getInstances()) {
                List<Tag> tags = instance.getTags();
                for (Tag tag: tags)
                    if (tag.getValue().equals("manager") &&
                    		instance.getState().getCode() == 16) return true;
        }
        return false;
    }
	static String getManagerAppScript(AWSCredentials credentials) {
        String strTemp="#!/bin/bash \n"
        		+ "wget  https://s3.amazonaws.com/sources-idan-and-yarden/ManagerApp.zip\n"
        		+ "unzip -P iDanyArden18 ManagerApp.zip \n"
        		+ "echo [default] > credentials\n"
        		+ "echo aws_access_key_id = " + credentials.getAWSAccessKeyId().toString() +" >> credentials\n"       		
        		+ "echo aws_secret_access_key = " + credentials.getAWSSecretKey().toString() +" >> credentials\n"
        		+ "java -jar ManagerApp.jar\n";
        String str = new String (Base64.encodeBase64(strTemp.getBytes()));
        return str;	
	}
	
	 public static void createFileFromInputStream(InputStream input, String output) throws IOException
	    {
	        OutputStream fos = new FileOutputStream("./" + output);
	        BufferedOutputStream out = new BufferedOutputStream(fos);
	        int count;
	        byte[] buffer = new byte[32000]; // more if you like but no need for it to be the entire file size
	        while ((count = input.read(buffer)) > 0)
	        {
	            out.write(buffer, 0, count);
	        }

	        out.close();
	    }
	   
}