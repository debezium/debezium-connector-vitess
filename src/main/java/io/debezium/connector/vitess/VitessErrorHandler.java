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
            LOGGER.error("Exception status: {}", exception.getStatus(), exception);
            return true;
        }
        return super.isRetriable(throwable);
    }
}
