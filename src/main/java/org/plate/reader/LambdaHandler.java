package org.plate.reader;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClient;
import com.amazonaws.services.rekognition.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LambdaHandler implements RequestHandler<S3Event, String> {

    private AmazonS3 s3client;
    private AmazonSQS sqsClient;
    private static final String SQS_DOWNWARD_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/945076052403/CaliLicenseDownwardQ";
    private static final Map<String, String> violations = Map.of("no_stop", "$300.00", "no_full_stop_on_right", "$75.00", "no_right_on_red", "$125.00" );
    private boolean caliRegistration = false;
    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        String bucketName = s3Event.getRecords().get(0).getS3().getBucket().getName();
        String fileName = s3Event.getRecords().get(0).getS3().getObject().getKey();
        try {
            initS3Client();
//            InputStream inputStream = s3client.getObject(bucketName, fileName).getObjectContent();
            String contentType  = s3client.getObject(bucketName, fileName).getObjectMetadata().getContentType();
//            String metadata  = String.valueOf(s3client.getObject(bucketName, fileName).getObjectMetadata());
            Map<String, String> metadata  = s3client.getObject(bucketName, fileName).getObjectMetadata().getUserMetadata();


            context.getLogger().log("====>>> FILE NAME ::: " + fileName +  ", BUCKET NAME ::: " + bucketName);
            context.getLogger().log("====>>> METADATA ::: " + metadata.toString());
            context.getLogger().log("====>>> CONTENT TYPE ::: " + contentType);


            AmazonRekognition rekognitionClient = new AmazonRekognitionClient();
            DetectTextRequest detectTextRequest = new DetectTextRequest()
                    .withImage(new Image().withS3Object(new S3Object().withName(fileName).withBucket(bucketName)));


            try {
                DetectTextResult detectTextResponse = rekognitionClient.detectText(detectTextRequest);
                System.out.println("Detected lines and words for " + fileName);
                boolean caliStateFlag = false;
                boolean caliLicenseNumber = false;

                for (TextDetection text : detectTextResponse.getTextDetections() ) {
                    String textFragment = text.getDetectedText();
                    if (textFragment.length() >= 7) {
                        if (!caliStateFlag && textFragment.equalsIgnoreCase("california")) {
                            caliStateFlag = true;
                        } else if (textFragment.length() == 7 && isValidString(textFragment)) {
                            caliLicenseNumber = true;
                        }
                    }
                    if (caliStateFlag && caliLicenseNumber) {
                        caliRegistration = true;
                        break;
                    }

                }
//                detectTextResponse.getTextDetections().forEach(text -> {
////                    boolean californiaState= false;
//
//                    if (!californiaState && text.getDetectedText().equalsIgnoreCase("california")){
//                        californiaState = true;
//                    }
//                    System.out.println("Detected: " + text.getDetectedText());
//                    System.out.println("Confidence: " + text.getConfidence());
//                    System.out.println("Id: " + text.getId());
////                    System.out.println("Parent Id: " + text.getParentId());
////                    System.out.println("Type: " + text.getType());
//                });
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }


            if (caliRegistration) {
                // Build the message to be sent to SQS
                JSONObject messageBody = new JSONObject();
                for (String key : metadata.keySet()) {
                    messageBody.put(key, metadata.get(key));
                }
                String addedFine = violations.get(metadata.get("type"));
                messageBody.put("fine",addedFine);

                // Send the message to SQS
                initSQSClient();
                SendMessageRequest sendMessageRequest = new SendMessageRequest()
                        .withQueueUrl(SQS_DOWNWARD_QUEUE_URL)
                        .withMessageBody(messageBody.toString());
                sqsClient.sendMessage(sendMessageRequest);

                context.getLogger().log("====>>> Message sent to Downward SQS: " + messageBody);
            } else {
                // TODO: event bridge for further processing
            }


        } catch (Exception ex) {
            context.getLogger().log("====>>> Exception occurred " + ex.getMessage());
            return "Error while reading file from S3 :::" + ex.getMessage();

        }
        return null;
    }

    private void initS3Client() {
        try {
            s3client = AmazonS3ClientBuilder
                    .standard()
                    .build();
        } catch(Exception ex){
        System.out.println("exception " + ex.getMessage());
        }

    }


    private void initSQSClient() {
        try {
            sqsClient = AmazonSQSClientBuilder.defaultClient();
        } catch(Exception ex){
            System.out.println("exception " + ex.getMessage());
        }

    }

    private static boolean isValidString(String input) {
        String regex = "^[0-9]*([A-Z][0-9]*){3}$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(input);

        return matcher.matches();
    }


}











