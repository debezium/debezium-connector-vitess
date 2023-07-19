/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.grpc.StatusRuntimeException;

public class VitessErrorHandler extends ErrorHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(VitessErrorHandler.class);

    public VitessErrorHandler(VitessConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(VitessConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            final StatusRuntimeException exception = (StatusRuntimeException) throwable;
            final String description = exception.getStatus().getDescription();
            LOGGER.info("Exception code: {} and description: {}", exception.getStatus().getCode(), description);
            switch (exception.getStatus().getCode()) {
                case CANCELLED:
                    // Try to match this description string:
                    // description=target: byuser.-4000.master: vttablet: rpc error: code = Canceled desc = grpc: the client connection is closing
                    if (description != null && description.contains("client connection is closing")) {
                        return true;
                    }
                    return false;
                case UNAVAILABLE:
                    return true;
                case UNKNOWN:
                    // Stream timeout error due to idle VStream or vstream ended unexpectedly.
                    if (description != null &&
                            (description.equals("stream timeout") ||
                                    description.contains("vstream ended unexpectedly") ||
                                    description.contains("unexpected server EOF"))) {
                        return true;
                    }
                    return false;
            }
        }
        return false;
    }
}
