package com.amazonaws.samples;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;


public class EC2Master {
	private AmazonEC2 ec2;
	private AWSCredentials credentials;
	ArrayList<Instance> instances;

	public EC2Master(AWSCredentials credentials ) {
		this.credentials = credentials;
		instances = new ArrayList<Instance>();
		ec2 = AmazonEC2ClientBuilder.standard().withEndpointConfiguration(new EndpointConfiguration("ec2.us-east-1.amazonaws.com","us-east-1")).withCredentials(new AWSStaticCredentialsProvider(credentials)).build();
	}
	
    public void createInstance(int numOfInstances) throws Exception {     
        try {
        	System.out.println("Creating " + numOfInstances + " EC2 intances..");
            RunInstancesRequest request = new RunInstancesRequest("ami-c58c1dd3", numOfInstances, numOfInstances);
            request.setInstanceType(InstanceType.T2Micro.toString());
            request.withUserData(encodeJars(this.credentials));
            
            List<Instance> newList = ec2.runInstances(request).getReservation().getInstances();
        	System.out.println("the new list" + newList);
            for (Instance newInstance : newList)
            	this.instances.add(newInstance);
    		System.out.println("Instances ----" + this.instances);

            System.out.println("Launched instances: " + instances);
            
        } catch (AmazonServiceException e) {
            System.out.println("Caught Exception: " + e.getMessage());
            System.out.println("Reponse Status Code: " + e.getStatusCode());
            System.out.println("Error Code: " + e.getErrorCode());
            System.out.println("Request ID: " + e.getRequestId());
        }   
 
    }
    
    static String encodeJars(AWSCredentials credentials) throws IOException{
        String strTemp="#!/bin/bash \n"
        		+ "wget https://s3.amazonaws.com/sources-idan-and-yarden/WorkerApp.zip\n"
        		+ "unzip -P iDanyArden17 WorkerApp.zip \n"
        		+ "echo [default] > credentials\n"
        		+ "echo aws_access_key_id = " + credentials.getAWSAccessKeyId().toString() +" >> credentials\n"       		
        		+ "echo aws_secret_access_key = " + credentials.getAWSSecretKey().toString() +" >> credentials\n"
        		+ "java -jar WorkerApp.jar\n";
        String str = new String (Base64.encodeBase64(strTemp.getBytes()));
        return str;	

    }
    
    static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }
    
    public int getNumOfInstances() {
    	int counter = -1;
    	DescribeInstancesRequest request = new DescribeInstancesRequest();
    	List<Reservation> result = ec2.describeInstances(request).getReservations();
    	for (Reservation reservation : result)
    		for (Instance instance : reservation.getInstances())
    			if (instance.getState().getCode() == 16)
    				counter ++;
    	return counter;
        }
    
    public void terminateWorkerInstances() {
		ArrayList<String> instanceIDs = new ArrayList<String>();
		System.out.println("Instances ----" + this.instances);
    	for (Instance instance : this.instances)
    	{
    		instanceIDs.add(instance.getInstanceId());
    	}
		ec2.terminateInstances(new TerminateInstancesRequest(instanceIDs));
    	System.out.println("workers are terminated");

    	
//    	DescribeInstancesRequest request = new DescribeInstancesRequest();
//    	List<Reservation> result = ec2.describeInstances(request).getReservations();
//		ArrayList<String> instanceIDs = new ArrayList<String>();
//    	for (Reservation reservation : result){
//    		for (Instance instance : reservation.getInstances()){
//    			List<Tag> tags = instance.getTags();
//    			if (tags.isEmpty() && instance.getState().getCode() == 16)
//    				instanceIDs.add(instance.getInstanceId());
//    			System.out.println("InstanceIDs size is :" + instanceIDs.size());
//    		}
//    		
//    	}
    	

    }
    
    public void terminateManager() {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        List<Reservation> result = ec2.describeInstances(request).getReservations();
        for (Reservation reservation : result) {
    		ArrayList<String> instanceIDs = new ArrayList<String>();
            for (Instance instance : reservation.getInstances()) {
                List<Tag> tags = instance.getTags();
                // TODO finish and fix.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
                for (Tag tag: tags) {
                    if (tag.getValue().equals("manager")){
                        if (instance.getState().getCode() == 16) {
                        }
                    }
                }

            }
        }
    }
}
