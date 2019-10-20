package com.csi.aws;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

public class AWSFileOperationS3 {

	String accesskey = "jjksnaknskan"; // replace with the valid value
	String secretKey = "jjksnaknskan"; // replace with the valid value
	static AmazonS3 s3Client;
	String clientRegion = "us-east-2";
	String path = "C:\\Users\\abdulla\\temp-folder\\temp";
	String bucketName = "csi-reg";
	static AWSFileOperation awsFileOperation;

	static {
		System.out.println("initializing s3...........");
		awsFileOperation = new AWSFileOperation();
		s3Client = getS3Bucket();
		System.out.println(".....initializing s3 success......");
	}

	public static void main(String[] args) {

		try (Scanner scanner = new Scanner(System.in)) {
			/* to download the bucket */

			System.out.println("Please enter 1 for download, 2 for upload");

			int option = scanner.nextInt();
			if (option == 1) {
				downloadBucket(s3Client, awsFileOperation.bucketName, null, awsFileOperation.path);
			} else if (option == 2) {
				// String tempFile = "dont-delete-this-file.txt";

				/* to upload the modified files into s3 bucket */
				awsFileOperation.uploadFiles(awsFileOperation.bucketName, null, awsFileOperation.path,
						DateUtil.tempFileName);
			} else {
				System.out.println("invalid option");
			}
			System.out.println(".........operation competed successfully...........");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	static AmazonS3 getS3Bucket() {

		BasicAWSCredentials awsCreds = new BasicAWSCredentials(awsFileOperation.accesskey, awsFileOperation.secretKey);
		if (s3Client == null) {
			s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.withRegion(awsFileOperation.clientRegion).build();
		}

		return s3Client;
	}

	public static void downloadBucket(AmazonS3 s3Client, String bucketName, String key_prefix, String pathToDownload)
			throws IOException {

		System.out.print("starting...........");
		TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(s3Client).build();// TransferManagerBuilder.standard().build();

		try {

			MultipleFileDownload xfer = xfer_mgr.downloadDirectory(bucketName, key_prefix, new File(pathToDownload));
			// XferMgrProgress.showTransferProgress(xfer);
			// XferMgrProgress.waitForCompletion(xfer);

			DateUtil.createUtilFile(pathToDownload, DateUtil.getCurrentSystemDate(DateUtil.format));
			System.out.println("..............ended");
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}

	}

	String downLoadTime;

	void uploadFiles(String bucketName, String key, String path, String tempFile) throws IOException {
		if (downLoadTime == null) {
			downLoadTime = DateUtil.readFileContent(awsFileOperation.path + "//" + DateUtil.tempFileName);
			if (downLoadTime == null) {
				return;
			}
		}

		File file = new File(path);
		String[] files = file.list();

		System.out.println("downLoadedTime ::" + downLoadTime);
		for (String fileName : files) {
			String lastModifiedFiletime = DateUtil.getFileLastModifiedDate(path + "//" + fileName, DateUtil.format);

			File file1 = new File(path + "//" + fileName);

			if (!file1.isDirectory()) {
				if (!downLoadTime.equals(lastModifiedFiletime) && !tempFile.equals(fileName)) {
					System.out.println(
							fileName + " :: lastModifiedFiletime :: " + lastModifiedFiletime + " path->  " + path);
					uploadFile(bucketName, key, fileName, path);
				}
			} else if (file1.isDirectory()) {
				uploadFiles(bucketName, key != null ? key + "/" + fileName : fileName, file1.getPath(), tempFile);
			}
		}

	}

	static void uploadFile(String bucketName, String key, String fileName, String path) {
		String fullPath = path + "\\" + fileName;

		System.out.println("fullPath ::" + fullPath);
		PutObjectRequest request = new PutObjectRequest(key == null ? bucketName : bucketName + "/" + key, fileName,
				new File(fullPath));
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentType("plain/text");
		metadata.addUserMetadata("x-amz-meta-title", "someTitle");
		request.setMetadata(metadata);
		s3Client.putObject(request);

	}

}

// package com.csi.aws;
//
// import java.io.File;
// import java.io.IOException;
//
// import com.amazonaws.AmazonServiceException;
// import com.amazonaws.auth.AWSStaticCredentialsProvider;
// import com.amazonaws.auth.BasicAWSCredentials;
// import com.amazonaws.services.s3.AmazonS3;
// import com.amazonaws.services.s3.AmazonS3ClientBuilder;
// import com.amazonaws.services.s3.model.ObjectMetadata;
// import com.amazonaws.services.s3.model.PutObjectRequest;
// import com.amazonaws.services.s3.transfer.MultipleFileDownload;
// import com.amazonaws.services.s3.transfer.TransferManager;
// import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
//
// public class AWSFileOperation {
//
// String accesskey = "";
// String secretKey = "";
// static AmazonS3 s3Client;
// String clientRegion = "us-east-2";
// String path = "C:\\Users\\AbhishekS\\Desktop\\abdulla\\aws\\tempFolder1";
// String bucketName = "csi-reg";
// static AWSFileOperation awsFileOperation;
//
// static {
// System.out.println("initializing s3...........");
// awsFileOperation = new AWSFileOperation();
// s3Client = getS3Bucket();
// System.out.println(".....initializing s3 success......");
// }
//
//
// public static void main(String[] args) {
// try {
//
// /* to download the bucket */
// //downloadBucket(s3Client, awsFileOperation.bucketName, null,
// awsFileOperation.path);
//
// //String tempFile = "dont-delete-this-file.txt";
//
// String downLoadTime = DateUtil.readFileContent(awsFileOperation.path + "//" +
// DateUtil.tempFileName);
//
// /* to upload the modified files into s3 bucket */
// awsFileOperation.uploadFiles(awsFileOperation.bucketName, null,
// awsFileOperation.path, DateUtil.tempFileName,
// downLoadTime);
// System.out.println(".........operation end...........");
// } catch (Exception e) {
// e.printStackTrace();
// }
// }
//
// static AmazonS3 getS3Bucket() {
//
// BasicAWSCredentials awsCreds = new
// BasicAWSCredentials(awsFileOperation.accesskey, awsFileOperation.secretKey);
// if (s3Client == null) {
// s3Client = AmazonS3ClientBuilder.standard().withCredentials(new
// AWSStaticCredentialsProvider(awsCreds))
// .withRegion(awsFileOperation.clientRegion).build();
// }
//
// return s3Client;
// }
//
// public static void downloadBucket(AmazonS3 s3Client, String bucketName,
// String key_prefix, String pathToDownload)
// throws IOException {
//
// System.out.print("starting...........");
// TransferManager xfer_mgr =
// TransferManagerBuilder.standard().withS3Client(s3Client).build();//
// TransferManagerBuilder.standard().build();
//
// try {
//
// MultipleFileDownload xfer = xfer_mgr.downloadDirectory(bucketName,
// key_prefix, new File(pathToDownload));
// // XferMgrProgress.showTransferProgress(xfer);
// // XferMgrProgress.waitForCompletion(xfer);
//
// DateUtil.createUtilFile(pathToDownload,
// DateUtil.getCurrentSystemDate(DateUtil.format));
// System.out.println("..............ended");
// } catch (AmazonServiceException e) {
// System.err.println(e.getErrorMessage());
// System.exit(1);
// }
//
// }
//
// void uploadFiles(String bucketName, String key, String path, String tempFile,
// String downLoadTime)
// throws IOException {
//
// File file = new File(path);
// String[] files = file.list();
//
// System.out.println("downLoadedTime ::" + downLoadTime);
// for (String fileName : files) {
// String lastModifiedFiletime = DateUtil.getFileLastModifiedDate(path + "//" +
// fileName, DateUtil.format);
//
// File file1 = new File(path + "//" + fileName);
//
// if (!file1.isDirectory()) {
// if (!downLoadTime.equals(lastModifiedFiletime) && !tempFile.equals(fileName))
// {
// System.out.println(fileName + " :: lastModifiedFiletime :: " +
// lastModifiedFiletime +" path-> "+ path);
// uploadFile(bucketName, key, fileName, path);
// }
// } else if (file1.isDirectory()) {
// uploadFiles(bucketName, key != null ? key+"/"+fileName : fileName,
// file1.getPath(), tempFile, downLoadTime);
// }
// }
//
// }
//
// static void uploadFile(String bucketName, String key, String fileName, String
// path) {
// String fullPath = path + "\\" + fileName;
//
// System.out.println("fullPath ::" + fullPath);
// PutObjectRequest request = new PutObjectRequest(key == null ? bucketName :
// bucketName+"/"+key, fileName, new File(fullPath));
// ObjectMetadata metadata = new ObjectMetadata();
// metadata.setContentType("plain/text");
// metadata.addUserMetadata("x-amz-meta-title", "someTitle");
// request.setMetadata(metadata);
// s3Client.putObject(request);
//
// }
//
// }
