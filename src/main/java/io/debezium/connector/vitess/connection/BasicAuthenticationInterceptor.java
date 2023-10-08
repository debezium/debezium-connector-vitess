package io.debezium.connector.vitess.connection;

import java.util.Base64;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.MethodDescriptor;

public class BasicAuthenticationInterceptor implements ClientInterceptor {

    private final String username;
    private final String password;

    public BasicAuthenticationInterceptor(String user, String pass) {
        username = user;
        password = pass;
    }

    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions,
                                                               final Channel channel) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
            @Override
            public void start(final Listener<RespT> responseListener, final Metadata headers) {
                String base64EncodedCredentials = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
                headers.put(Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Basic " + base64EncodedCredentials);
                super.start(responseListener, headers);
            }
        };
    }
}
