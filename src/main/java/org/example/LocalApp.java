package org.example;

import java.util.LinkedList;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.HadoopJarStepConfig;
import software.amazon.awssdk.services.emr.model.JobFlowInstancesConfig;
import software.amazon.awssdk.services.emr.model.PlacementType;
import software.amazon.awssdk.services.emr.model.RunJobFlowRequest;
import software.amazon.awssdk.services.emr.model.RunJobFlowResponse;
import software.amazon.awssdk.services.emr.model.StepConfig;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;

public class LocalApp{
    private final static software.amazon.awssdk.regions.Region region = Region.US_EAST_1;
    public static final EmrClient emrClient = EmrClient.builder().region(region).build();
    public static final S3Client s3Client = S3Client.builder().region(region).build();

    public static void main(String[]args){

        //createBucket(s3Client, "bucketurevich");

        LinkedList<StepConfig> stepsConfigs = new LinkedList<>();
        for(int i = 1; i <= 2; i++){
            stepsConfigs.add(configureStep("s3://bucketurevich/step" + i + ".jar", "step" + i));
         }
        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(9)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("2.7.3")
                .ec2KeyName("vockey")
                .placement(PlacementType.builder().build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("hypernym")
                .instances(instances)
                .steps(stepsConfigs.get(0),stepsConfigs.get(1))
                .logUri("s3n://bucketurevich/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();

        RunJobFlowResponse response = emrClient.runJobFlow(request);
        String id = response.jobFlowId();
        System.out.println("Ran job flow with id: " + id);
    }
    private static StepConfig configureStep(String pathInBucket, String stepName) {

        HadoopJarStepConfig step = HadoopJarStepConfig.builder()
                .jar(pathInBucket)
                .build();

        StepConfig stepConfig = StepConfig.builder()
                .name(stepName)
                .hadoopJarStep(step)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        return stepConfig;
    }

    public static void createBucket( S3Client s3Client, String bucketName) {

        
        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                .bucket(bucketName)
                .build();

            s3Client.createBucket(bucketRequest);
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                .bucket(bucketName)
                .build();

            // Wait until the bucket is created and print out the response.
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
            System.out.println(bucketName +" is ready");

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}