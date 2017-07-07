package com.amazonaws.samples;

import com.amazonaws.AmazonClientException;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.*;
import java.net.URL;

public class S3Obj {
    private File object;
    //private String fileName;
    private BufferedReader reader;
    private String bucketName;
    private String fileKey;
    private AmazonS3 s3;


    public S3Obj(String bucketName, String fileKey ) {
        createS3();
        this.bucketName = bucketName;
        this.fileKey = fileKey;
        try {
            createFile(s3.getObject(new GetObjectRequest(bucketName, fileKey)).getObjectContent());
        } catch (IOException e) {
            e.printStackTrace();
        }
        object = new File("./tempInput.txt");
        try {
            reader = new BufferedReader(new FileReader(object));
            

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public S3Obj(String bucketName, String fileKey, String fileName ) {
        createS3();
        this.bucketName = bucketName;
        this.fileKey = fileKey;
        upload(fileName);
    }
//    
//    public S3Obj(String bucketName, String fileKey, String fileName) {
//        createS3();
//        this.bucketName = bucketName;
//        this.fileKey = fileKey;
//        this.fileName = fileName;
//    }

    //TODO check that it works fine
    public int countNumOfLines() {
        // count the lines:
    	int lines = 0;
    	try {
    		BufferedReader reader = new BufferedReader(new FileReader(this.object));
            while (reader.readLine() != null) lines++;
            reader.close();
    	}
    	catch (FileNotFoundException e){
    		e.printStackTrace();
    	}
    	catch (IOException e2){
    		e2.printStackTrace();
    	}
        
        return lines;
    }
    public String getBucketName() {
        return bucketName;
    }

    public String getFileKey() {
        return fileKey;
    }

    
    public synchronized void upload(String fileName) {
    System.out.println("Uploading a new object to S3 from a file\n");
    File file = new File(fileName);
    s3.putObject(new PutObjectRequest(bucketName, fileKey, file));
    }
    

    
    // Creates a temp file to store the data from the s3
    public synchronized static void createFile(InputStream input) throws IOException
    {
        OutputStream fos = new FileOutputStream("./tempInput.txt");
        BufferedOutputStream out = new BufferedOutputStream(fos);
        int count;
        byte[] buffer = new byte[32000]; 
        while ((count = input.read(buffer)) > 0)
        {
            out.write(buffer, 0, count);
        }

        out.close();
    }
    
    public synchronized String read()  {
    	String line = null;
        try {
        	line = reader.readLine();
        	
        }
        catch (IOException e){
        	e.printStackTrace();
        }
        return line;	
    }

    private void createS3(){

        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("./credentials","default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (/users/studs/bsc/2015/davidzag/.aws/credentials), and is in valid format.",
                    e);
        }

        Region usEast1 = Region.getRegion(Regions.US_EAST_1);
        s3 = new AmazonS3Client(credentials);
        s3.setRegion(usEast1);

    }
    
    public String getPublicUrl() {
    	java.util.Date expiration = new java.util.Date();
    	long msec = expiration.getTime();
    	msec += 1000 * 60 * 60; // 1 hour.
    	expiration.setTime(msec);
    	             
    	GeneratePresignedUrlRequest generatePresignedUrlRequest = 
    	              new GeneratePresignedUrlRequest(this.bucketName, this.fileKey);
    	generatePresignedUrlRequest.setMethod(HttpMethod.GET); // Default.
    	generatePresignedUrlRequest.setExpiration(expiration);
    	             
    	URL url = s3.generatePresignedUrl(generatePresignedUrlRequest); 
    	return url.toString();
    }
}

