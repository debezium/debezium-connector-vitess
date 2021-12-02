/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import static io.grpc.Status.Code.UNAVAILABLE;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.grpc.StatusRuntimeException;

public class VitessErrorHandler extends ErrorHandler {
    public VitessErrorHandler(String logicalName, ChangeEventQueue<?> queue) {
        super(VitessConnector.class, logicalName, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof StatusRuntimeException) {
            final StatusRuntimeException exception = (StatusRuntimeException) throwable;
            return exception.getStatus().getCode().equals(UNAVAILABLE);
        }
        return false;
    }
}
