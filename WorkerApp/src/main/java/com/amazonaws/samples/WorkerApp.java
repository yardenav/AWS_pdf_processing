package com.amazonaws.samples;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Stack;

import org.apache.commons.io.FileUtils;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.util.ImageIOUtil;
import org.apache.pdfbox.util.PDFTextStripper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.model.Message;

public class WorkerApp {

	public static void main(String[] args) {
		 AWSCredentials credentials = null;
	        try {
	            credentials = new ProfileCredentialsProvider("./credentials","default").getCredentials();
	        } catch (Exception e) {
	            throw new AmazonClientException(
	                    "Cannot load the credentials from the credential profiles file. " +
	                    "Please make sure that your credentials file is at the correct " +
	                    "location (C:\\Users\\win7\\.aws\\credentials), and is in valid format.",
	                    e);
	        }
		SQSMaster sqsMaster = new SQSMaster(credentials);
		
		while (true) {
			List<Message> messages = sqsMaster.getSQSMessage("JobsQueue");
        	if (!messages.isEmpty()) {
        		Message message = messages.get(0);
            	if (message.getBody().equals("singlePdfTask")) {	
            		String workType = message.getMessageAttributes().get("WorkType").getStringValue();
            		String url = message.getMessageAttributes().get("URL").getStringValue();
            		String taskId = message.getMessageAttributes().get("TaskID").getStringValue();
            		String pdfJobID = message.getMessageAttributes().get("PdfJobID").getStringValue();
            		String bucketName = message.getMessageAttributes().get("BucketName").getStringValue();
            		
            		File pdfFile;
					String pdfStr;
					String resUrl;
					try {
						pdfFile = downloadPdfFile(url);
						pdfStr = pdfToString(pdfFile);
					} catch (Exception e1) {
						workType = "Error";
						pdfStr = null;
						pdfFile = null;
					}
            		S3Obj resultObj ;
            		switch (workType.charAt(2)){
            			case 'T': {
            				// Text
            				try {
								PrintWriter out = new PrintWriter("./tempResultFile.txt");
	            				out.print(pdfStr);
	            				out.close();
	        					resultObj = new S3Obj(bucketName, pdfJobID,"./tempResultFile.txt" );
	        					resUrl = resultObj.getPublicUrl();
            				} 
            				catch (FileNotFoundException e) {
            					resUrl = "ERROR: FILE NOT FOUND";
            					//e.printStackTrace();
            				}
							break;
            			}
            			case 'H' : {// HTML
        					try {
								resultObj = new S3Obj(bucketName, pdfJobID,createHTMLFromString(pdfStr) );
								resUrl = resultObj.getPublicUrl();
							} catch (Exception e) {
								resUrl = "ERROR: CANNOT CONVERT PDF TO HTML";
							}
            				break;
            			}
            			case 'I': {// Image
        					try {
								resultObj = new S3Obj(bucketName, pdfJobID,convertToImage(pdfFile) );
								resUrl = resultObj.getPublicUrl();
							} catch (Exception e) {
								// TODO Auto-generated catch block
								resUrl = "ERROR: CANNOT CONVERT PDF TO IMAGE";
							}
            				break;
            			}
            			default : 
    						resUrl = "ERROR : FILE/URL Problem";
        					break;
            		
            				
            		}
            		
            		
            		// Sending Message to manager
            		String[] originalUrl = {"OriginalURL",url};
            		String[] workType2 = {"WorkType",workType};
            		String[] resultUrl = {"ResultURL", resUrl}; // the taskID received from the local user.
            		String[] taskId2 = {"TaskID",taskId};


            		Stack<String[]> attr = new Stack<String[]>();
            		attr.push(originalUrl);
            		attr.push(workType2);
            		attr.push(resultUrl);
            		attr.push(taskId2);
            		sqsMaster.sendSQSMessage("pdfDoneQueue", "PdfDoneTask", attr);
            		
            		sqsMaster.deleteMessage(message, "JobsQueue");
            		System.out.println("Job is done and message deleted");	            		
            	} 
        	}
		}
		
		//S3Obj s3 = new S3Obj(");
		
		
		
	}


	private static String pdfToString(File file){
		try {
	        PDFTextStripper pdfStripper = null;
	        PDDocument pdDoc = null;
	        COSDocument cosDoc = null;
	        PDFParser parser = new PDFParser(new FileInputStream(file));
	        parser.parse();
	        cosDoc = parser.getDocument();
	        pdfStripper = new PDFTextStripper();
	        pdDoc = new PDDocument(cosDoc);
	        pdfStripper.setStartPage(1);
	        pdfStripper.setEndPage(1);
	        String pdfString = pdfStripper.getText(pdDoc);
	        cosDoc.close();
	        return pdfString;
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	private static File downloadPdfFile(String urlStr) throws Exception{
		File destination = new File("./tempPdf.pdf");
		try {
			URL url = new URL(urlStr);
			FileUtils.copyURLToFile(url, destination);
		} catch (MalformedURLException e) {
			throw e;
		} catch (IOException e) {
			throw e;
		}
		return destination;
	}

	
	private static String createHTMLFromString(String pdfStr) throws Exception{
		File htmlTemplateFile = new File("./htmlTemplate.html");
		
		try {
			String htmlString = FileUtils.readFileToString(htmlTemplateFile,Charset.forName("UTF-8"));	
			String title = "PDF to HTML";
			htmlString = htmlString.replace("$title", title);
			htmlString = htmlString.replace("$body", pdfStr);
			File newHtmlFile = new File("./tempOutput.html");
			FileUtils.writeStringToFile(newHtmlFile, htmlString,Charset.forName("UTF-8"));
			return "./tempOutput.html";
		} catch (IOException e) {
			throw e;
		}
		
	}
	
	private static String convertToImage(File file) throws Exception{
		try {
			PDDocument document = PDDocument.loadNonSeq(file, null);
			PDPage pdPage = (PDPage) document.getDocumentCatalog().getAllPages().get(0);
			BufferedImage bim = pdPage.convertToImage(BufferedImage.TYPE_INT_RGB, 300);
			ImageIOUtil.writeImage(bim, "./resultImage.png", 300);
			document.close();
		} catch (IOException e) {
			throw e;
		}
		return "./resultImage.png";
	}
}
