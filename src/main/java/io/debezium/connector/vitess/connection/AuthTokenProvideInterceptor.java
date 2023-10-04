package io.debezium.connector.vitess.connection;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;

public class AuthTokenProvideInterceptor implements ClientInterceptor {

    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                                                               final Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                System.out.println("PHANI : Setting authorization header");
                headers.put(Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Basic cHJvZHZzdHJlYW06YmFy");
                super.start(responseListener, headers);
            }
        };
    }
}