package org.apache.flink.connector.aws.sink.throwable;

import org.apache.flink.connector.base.sink.throwable.FatalExceptionClassifier;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AWSExceptionHandlerTest {

    private final RuntimeException mappedException = new RuntimeException("AWSExceptionHandlerTest");

    private final AWSExceptionHandler exceptionHandler = AWSExceptionHandler.withClassifier(
            FatalExceptionClassifier.withRootCauseOfType(
                    UnsupportedOperationException.class,
                    (err) -> mappedException
            )
    );

    @Test
    public void shouldReturnTrueIfFatal() {
        assertTrue(exceptionHandler.consumeIfFatal(new UnsupportedOperationException(), (err) -> {}));
    }

    @Test
    public void shouldReturnFalseIfNonFatal() {
        assertFalse(exceptionHandler.consumeIfFatal(new IndexOutOfBoundsException(), (err) -> {}));
    }

    @Test
    public void shouldConsumeMappedExceptionIfFatal() {
        Set<Exception> consumedExceptions = new HashSet<>();
        assertTrue(exceptionHandler.consumeIfFatal(
                new UnsupportedOperationException(),
                consumedExceptions::add)
        );

        assertEquals(1, consumedExceptions.size());
        assertTrue(consumedExceptions.contains(mappedException));
    }

    @Test
    public void shouldNotConsumeMappedExceptionIfNonFatal() {
        assertFalse(exceptionHandler.consumeIfFatal(
                new IndexOutOfBoundsException(),
                // consumer should not be called
                (err) -> assertTrue(false))
        );
    }
}
