/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.grpc.StatusRuntimeException;

public class VitessErrorHandler extends ErrorHandler {
    public VitessErrorHandler(VitessConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(VitessConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            final StatusRuntimeException exception = (StatusRuntimeException) throwable;
            switch (exception.getStatus().getCode()) {
                case CANCELLED:
                case UNAVAILABLE:
                    return true;
                case UNKNOWN:
                    String description = exception.getStatus().getDescription();
                    // Stream timeout error due to idle VStream.
                    if (description != null && description.equals("stream timeout")) {
                        return true;
                    }
            }
        }
        return false;
    }
}
