package org.apache.flink.connector.aws.sink.throwable;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.sts.model.StsException;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class AWSExceptionClassifierUtilTest {

    @Test
    public void shouldCreateFatalExceptionClassifierThatClassifiesAsFatalIfMatchingErrorCode() {
        // given
        AwsServiceException exception = StsException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("NotAuthorizedException").build())
                .build();

        FatalExceptionClassifier classifier = AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                StsException.class,
                "NotAuthorizedException",
                (err) -> new RuntimeException()
        );

        // when (FatalExceptionClassifier#isFatal returns false if exception is fatal)
        boolean isFatal = !classifier.isFatal(exception, (err) -> {});

        // then
        assertTrue(isFatal);
    }

    @Test
    public void shouldCreateFatalExceptionClassifierThatClassifiesAsNonFatalIfNotMatchingErrorCode() {
        // given
        AwsServiceException exception = StsException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("SomeOtherException").build())
                .build();

        FatalExceptionClassifier classifier = AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                StsException.class,
                "NotAuthorizedException",
                (err) -> new RuntimeException()
        );

        // when (FatalExceptionClassifier#isFatal returns true if exception is non-fatal)
        boolean isFatal = !classifier.isFatal(exception, (err) -> {});

        // then
        assertFalse(isFatal);
    }

    @Test
    public void shouldCreateFatalExceptionClassifierThatAppliesThrowableMapper() {
        // given
        AwsServiceException exception = StsException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("NotAuthorizedException").build())
                .build();

        Exception mappedException = new RuntimeException("shouldCreateFatalExceptionClassifierThatAppliesThrowableMapper");
        FatalExceptionClassifier classifier = AWSExceptionClassifierUtil.withAWSServiceErrorCode(
                StsException.class,
                "NotAuthorizedException",
                (err) -> mappedException
        );

        Set<Exception> consumedExceptions = new HashSet<>();

        // when
        classifier.isFatal(exception, consumedExceptions::add);

        // then mappedException has been consumed
        assertEquals(1, consumedExceptions.size());
        assertTrue(consumedExceptions.contains(mappedException));
    }
}
