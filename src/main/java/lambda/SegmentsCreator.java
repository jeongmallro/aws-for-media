package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.*;

/**
 * Apple HTTP Live Streaming(HLS) muxer
 * 원본 동영상(mp4 등) -> HLS 변환 -> m3u8, ts
 */
public class SegmentsCreator implements RequestHandler<S3Event, String> {
    private final String DESTINATION_BUCKET_NAME = "DESTINATION_BUCKET_NAME";
    private final String DESTINATION_BUCKET_FOLDER = "DESTINATION_BUCKET_FOLDER";
    private final String LAMBDA_STORAGE_PATH = "/tmp";
    private final String FFMPEG_PATH = "/opt/bin/ffmpeg";
    private final String M3U8_EXTENSION = "m3u8";
    private final String TS_EXTENSION = "ts";

    @Override
    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();

        S3Client s3Client = S3Client.builder()
                .region(Region.AP_NORTHEAST_2)
                .build();

        try {
            // S3에 새로 생성된 원본 동영상 파일 가져오기
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getUrlDecodedKey();
            InputStream object = getObject(s3Client, srcBucket, srcKey);
            String videoName = getVideoName(srcKey);
            String videoExtension = getFileExtension(srcKey);

            // Lambda /tmp에 해당 파일 저장
            String inputPath = createInputPath(videoName, videoExtension);
            saveVideoInTmp(object, inputPath);

            // Ffmpeg 명령문 생성 및 실행
            String tsOutputFormat = createTsOutputFormat(videoName);
            String m3u8OutputFormat = createM3u8OutputFormat(videoName);
            String[] ffmpegCmdArray = generateFfmpegCmdArray(tsOutputFormat, m3u8OutputFormat, inputPath);

            logger.log("Video encoding start");
            Process process = Runtime.getRuntime().exec(ffmpegCmdArray);
            process.waitFor();
            logger.log("Video encoding completed");

            // 세그먼트 파일들 S3에 저장
            saveSegmentsInDestinationBucket(logger, s3Client, videoName);

            return "ok";
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void saveSegmentsInDestinationBucket(LambdaLogger logger, S3Client s3Client, String videoName) throws FileNotFoundException {
        File directory = new File(LAMBDA_STORAGE_PATH);
        File[] files = directory.listFiles();

        for (File file : files) {
            String originalName = file.getName();
            String fileName = getVideoName(originalName);
            String fileExtension = getFileExtension(originalName);

            if (validateFileExtension(fileExtension) && validateFileName(fileName, videoName)) {
                logger.log("Found file: " + originalName);
                long length = file.length();
                InputStream stream = new FileInputStream(file);
                putObject(s3Client, originalName, logger, stream, length);
            }
        }
    }

    private String[] generateFfmpegCmdArray(String tsOutputFormat, String m3u8OutputFormat, String inputPath) {
        String[] ffmpegCmdArray = {
                FFMPEG_PATH,
                "-i", inputPath,
                "-s", "1080x720",
                "-hls_time", "10",
                "-hls_list_size", "0",
                "-hls_segment_filename", tsOutputFormat,
                "-f", "hls",
                m3u8OutputFormat
        };
        return ffmpegCmdArray;
    }

    public void saveVideoInTmp(InputStream inputStream, String inputPath)  throws IOException{
        File file = new File(inputPath);

        // 파일이 존재하지 않으면 생성
        if (!file.exists()) {
            file.createNewFile();
        }

        // InputStream을 /tmp에 파일로 저장
        try (OutputStream outputStream = new FileOutputStream(file)) {
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
        } finally {
            inputStream.close();  // InputStream을 닫음
        }
    }

    private InputStream getObject(S3Client s3Client, String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        return s3Client.getObject(getObjectRequest);
    }

    private void putObject(S3Client s3Client, String key, LambdaLogger logger, InputStream stream, long length) {
        String fullKeyName = DESTINATION_BUCKET_FOLDER + "/" + key;

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(DESTINATION_BUCKET_NAME)
                .key(fullKeyName)
                .contentLength(length)
                .build();

        logger.log("Writing to: " + DESTINATION_BUCKET_NAME + "/" + fullKeyName);
        try {
            s3Client.putObject(putObjectRequest,
                    RequestBody.fromInputStream(stream, length));
        }
        catch(AwsServiceException e)
        {
            logger.log(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private String createTsOutputFormat(String videoName) {
        return LAMBDA_STORAGE_PATH + "/" + videoName + "_%03d" + "." + TS_EXTENSION;
    }

    private String createM3u8OutputFormat(String videoName) {
        return LAMBDA_STORAGE_PATH + "/" + videoName + "." + M3U8_EXTENSION;
    }

    private String createInputPath(String videoName, String videoExtension) {
        return LAMBDA_STORAGE_PATH + "/" + videoName + "." + videoExtension;
    }

    private String getVideoName(String originalName) {
        return originalName.split("\\.")[0];
    }

    private String getFileExtension(String originalName) {
        return originalName.substring(originalName.lastIndexOf(".") + 1);
    }

    private boolean validateFileExtension(String fileExtension) {
        return fileExtension.equals(M3U8_EXTENSION) || fileExtension.equals(TS_EXTENSION);
    }

    private boolean validateFileName(String fileName, String videoName) {
        return fileName.startsWith(videoName);
    }

}
