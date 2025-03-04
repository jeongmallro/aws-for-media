package lambda;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.imageio.ImageIO;

import net.coobird.thumbnailator.Thumbnails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.S3Client;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;

/**
 * ImageResizer
 * 원본 이미지 -> 이미지 리사이징 -> 리사이즈된 이미지
 */
public class ImageResizer implements RequestHandler<S3Event, String> {
    private final String REGEX = ".*\\.([^\\.]*)";
    private final String JPG_TYPE = "jpg";
    private final String JPEG_TYPE = "jpeg";
    private final String JPG_MIME = "image/jpeg";
    private final String PNG_TYPE = "png";
    private final String PNG_MIME = "image/png";
    private final String DST_BUCKET_NAME = "DST_BUCKET_NAME";
    @Override
    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();

        S3Client s3Client = S3Client.builder()
                .region(Region.AP_NORTHEAST_2)
                .build();

        try {
            S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

            String dstBucket = DST_BUCKET_NAME;
            String dstKey = "resized-" + srcKey;

            // Infer the image type.
            Matcher matcher = Pattern.compile(REGEX).matcher(srcKey);
            if (!matcher.matches()) {
                logger.log("Unable to infer image type for key " + srcKey);
                return "";
            }
            String imageType = matcher.group(1);
            if (!(JPG_TYPE.equals(imageType)) && !(PNG_TYPE.equals(imageType)) && !(JPEG_TYPE.equals(imageType))) {
                logger.log("Skipping non-image " + srcKey);
                return "";
            }

            // Download the image from S3 into a stream
            InputStream s3Object = getObject(s3Client, srcBucket, srcKey);

            // Read the source image and resize it
            BufferedImage srcImage = ImageIO.read(s3Object);
            BufferedImage newImage = resizeImage(srcImage);

            // Re-encode image to target format
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            ImageIO.write(newImage, imageType, outputStream);

            // Upload new image to S3
            putObject(s3Client, outputStream, dstBucket, dstKey, imageType, logger);

            logger.log("Successfully resized " + srcBucket + "/"
                    + srcKey + " and uploaded to " + dstBucket + "/" + dstKey);
            return "ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getObject(S3Client s3Client, String bucket, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        return s3Client.getObject(getObjectRequest);
    }

    private void putObject(S3Client s3Client, ByteArrayOutputStream outputStream,
                           String bucket, String key, String imageType, LambdaLogger logger) {
        long contentLength = outputStream.size();
        String contentType = "";
        if (JPG_TYPE.equals(imageType)) {
            contentType = JPG_MIME;
        } else if (PNG_TYPE.equals(imageType)) {
            contentType = PNG_MIME;
        }

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .contentLength(contentLength)
                .build();

        // Uploading to S3 destination bucket
        logger.log("Writing to: " + bucket + "/" + key);
        try {
            s3Client.putObject(putObjectRequest,
                    RequestBody.fromBytes(outputStream.toByteArray()));
        } catch(AwsServiceException e) {
            logger.log(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    private BufferedImage resizeImage(BufferedImage srcImage) throws IOException {
        BufferedImage resizedImage = Thumbnails.of(srcImage)
                .size(223, 223)
                .asBufferedImage();

        return resizedImage;
    }
}