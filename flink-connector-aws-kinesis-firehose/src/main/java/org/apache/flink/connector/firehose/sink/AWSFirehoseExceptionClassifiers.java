package org.apache.flink.connector.firehose.sink;

import org.apache.flink.connector.aws.sink.throwable.AWSExceptionClassifierUtil;
import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import software.amazon.awssdk.services.firehose.model.FirehoseException;
import software.amazon.awssdk.services.firehose.model.ResourceNotFoundException;

/** Class containing set of {@link FatalExceptionClassifier} for
 * {@link software.amazon.awssdk.services.firehose.model.FirehoseException}.
 **/
public class AWSFirehoseExceptionClassifiers {

    public static FatalExceptionClassifier getNotAuthorizedExceptionClassifier() {
        return AWSExceptionClassifierUtil.withAWSServiceErrorCode(FirehoseException.class, "NotAuthorized",
                err -> new KinesisFirehoseException("Encountered non-recoverable exception: NotAuthorized", err));
    }

    public static FatalExceptionClassifier getAccessDeniedExceptionClassifier() {
        return AWSExceptionClassifierUtil.withAWSServiceErrorCode(FirehoseException.class, "AccessDeniedException",
                err -> new KinesisFirehoseException("Encountered non-recoverable exception: AccessDeniedException", err));

    }

    public static FatalExceptionClassifier getResourceNotFoundExceptionClassifier() {
        return FatalExceptionClassifier.withRootCauseOfType(ResourceNotFoundException.class,
                err -> new KinesisFirehoseException(
                        "Encountered non-recoverable exception relating to not being able to find the specified resources", err
                ));
    }
}

