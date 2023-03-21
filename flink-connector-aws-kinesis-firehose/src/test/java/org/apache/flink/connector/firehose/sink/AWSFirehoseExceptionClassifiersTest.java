package org.apache.flink.connector.firehose.sink;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;

import static org.junit.jupiter.api.Assertions.assertFalse;

public class AWSFirehoseExceptionClassifiersTest {

    private final FatalExceptionClassifier classifier = FatalExceptionClassifier.createChain(
            AWSFirehoseExceptionClassifiers.getAccessDeniedExceptionClassifier(),
            AWSFirehoseExceptionClassifiers.getNotAuthorizedExceptionClassifier(),
            AWSFirehoseExceptionClassifiers.getResourceNotFoundExceptionClassifier()
    );

    @Test
    public void shouldClassifyNotAuthorizedAsFatal() {
        AwsServiceException firehoseException = FirehoseException.builder()
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorCode("NotAuthorized").build()
                )
                .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(firehoseException, ex -> {}));
    }

    @Test
    public void shouldClassifyAccessDeniedExceptionAsFatal() {
        AwsServiceException firehoseException = FirehoseException.builder()
                .awsErrorDetails(
                        AwsErrorDetails.builder().errorCode("AccessDeniedException").build()
                )
                .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(firehoseException, ex -> {}));
    }

    @Test
    public void shouldClassifyResourceNotFoundAsFatal() {
        AwsServiceException firehoseException = ResourceNotFoundException.builder()
                .build();

        // isFatal returns `true` if an exception is non-fatal
        assertFalse(classifier.isFatal(firehoseException, ex -> {}));
    }

}
