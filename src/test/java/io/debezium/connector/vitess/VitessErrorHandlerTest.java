/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.Test;

import io.debezium.junit.logging.LogInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class VitessErrorHandlerTest {

    @Test
    public void shouldLogMessageWithCodeAndDescription() {
        String expectedDescription = "target: byuser.-4000.master: vttablet: rpc error: code = " +
                "Canceled desc = grpc: the client connection is closing";
        String expectedStatusCodeString = "CANCELLED";
        final LogInterceptor logInterceptor = new LogInterceptor(VitessErrorHandler.class);
        Status status = Status.CANCELLED.withDescription(expectedDescription);
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        vitessErrorHandler.isRetriable(notFoundException);
        assertThat(logInterceptor.containsErrorMessage(expectedStatusCodeString)).isTrue();
        assertThat(logInterceptor.containsErrorMessage(expectedDescription)).isTrue();
    }

    @Test
    public void shouldRetryIOException() {
        IOException ioException = new IOException();
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(ioException)).isTrue();
    }

    @Test
    public void shouldNotRetryNonIONonStatusRuntimeExceptions() {
        IllegalStateException illegalStateException = new IllegalStateException();
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(illegalStateException)).isFalse();
    }

    @Test
    public void shouldRetryCancelledOnClosedClient() {
        Status status = Status.CANCELLED.withDescription("target: byuser.-4000.master: vttablet: rpc error: code = " +
                "Canceled desc = grpc: the client connection is closing");
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryCancelledWithOtherDescription() {
        Status status = Status.CANCELLED.withDescription("any other cancel");
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryCancelled() {
        Status status = Status.CANCELLED;
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryNotFoundWithTabletDownDescription() {
        Status status = Status.NOT_FOUND.withDescription("tablet: cell:\"cell_1\" uid:123 is either down or nonexistent");
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryNotFound() {
        StatusRuntimeException notFoundException = new StatusRuntimeException(Status.NOT_FOUND);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryUnavailable() {
        StatusRuntimeException notFoundException = new StatusRuntimeException(Status.UNAVAILABLE);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryUnknownWithStreamTimeoutDescription() {
        Status status = Status.UNKNOWN.withDescription("stream timeout");
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryUnknownWithStreamEndedUnexpectedly() {
        Status status = Status.UNKNOWN.withDescription("vstream ended unexpectedly");
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

    @Test
    public void shouldRetryUnknown() {
        Status status = Status.UNKNOWN;
        StatusRuntimeException notFoundException = new StatusRuntimeException(status);
        VitessErrorHandler vitessErrorHandler = new VitessErrorHandler(null, null, null);
        assertThat(vitessErrorHandler.isRetriable(notFoundException)).isTrue();
    }

}
