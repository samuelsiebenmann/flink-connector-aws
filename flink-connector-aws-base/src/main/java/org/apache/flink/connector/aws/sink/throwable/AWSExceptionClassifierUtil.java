package org.apache.flink.connector.aws.sink.throwable;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.apache.flink.util.ExceptionUtils;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * Util class to create {@link FatalExceptionClassifier} to classify {@link AwsServiceException} based on
 * {@link AwsErrorDetails#errorCode()}.
 */
public class AWSExceptionClassifierUtil {

    /**
     * Creates a {@link FatalExceptionClassifier} that classifies an exception as fatal if a given exception contains
     * an {@link AwsServiceException} of type serviceExceptionType and {@link AwsErrorDetails#errorCode()} errorCode.
     *
     * @param serviceExceptionType The specific {@link AwsServiceException} to look for in the exception.
     * @param errorCode The {@link AwsErrorDetails#errorCode()} for the passed serviceExceptionType.
     * @param mapper The exception mapper to be used by the returned {@link AWSExceptionHandler}.
     * @return A {@link FatalExceptionClassifier} classifying based on exception type and error code.
     */
    public static FatalExceptionClassifier withAWSServiceErrorCode(Class<? extends AwsServiceException> serviceExceptionType,
                                                                   String errorCode,
                                                                   Function<Throwable, Exception> mapper) {
        return new FatalExceptionClassifier((err) -> {
            Optional<? extends AwsServiceException> exceptionOptional = ExceptionUtils.findThrowable(err, serviceExceptionType);
            if (exceptionOptional.isEmpty()) {
                return false;
            }

            AwsServiceException exception = exceptionOptional.get();
            return exception.awsErrorDetails() != null && Objects.equals(errorCode, exception.awsErrorDetails().errorCode());
        }, mapper);
    }
}
