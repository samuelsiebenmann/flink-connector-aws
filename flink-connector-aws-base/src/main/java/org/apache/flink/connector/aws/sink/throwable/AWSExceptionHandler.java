package org.apache.flink.connector.aws.sink.throwable;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;

import java.util.function.Consumer;

/**
 * Class to provide improved semantics over {@link FatalExceptionClassifier#isFatal(Throwable, Consumer)}.
 * {@link FatalExceptionClassifier#isFatal(Throwable, Consumer)} returns `false` for a fatal exception and `true` for a
 * non-fatal exception.
 */
public class AWSExceptionHandler {

    private final FatalExceptionClassifier classifier;

    private AWSExceptionHandler(FatalExceptionClassifier classifier) {
        this.classifier = classifier;
    }

    public static AWSExceptionHandler withClassifier(FatalExceptionClassifier classifier) {
        return new AWSExceptionHandler(classifier);
    }

    /**
     * Passes a given {@link Throwable} t to a given {@link Consumer<Exception>} consumer if the throwable is fatal.
     * Returns `true` if the {@link Throwable} has been passed to the {@link Consumer<Exception>} (i.e. it is fatal)
     * and `false` otherwise.
     *
     * @param t a {@link Throwable}
     * @param consumer a {@link Consumer<Exception>} to call if the passed throwable t is fatal.
     * @return `true` if t is fatal, `false` otherwise.
     */
    public boolean consumeIfFatal(Throwable t, Consumer<Exception> consumer) {
        return !classifier.isFatal(t, consumer);
    }
}
