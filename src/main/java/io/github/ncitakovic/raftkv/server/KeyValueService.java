package io.github.ncitakovic.raftkv.server;

import com.google.protobuf.ByteString;
import io.github.ncitakovic.raftkv.api.v1.DeleteRequest;
import io.github.ncitakovic.raftkv.api.v1.DeleteResponse;
import io.github.ncitakovic.raftkv.api.v1.GetRequest;
import io.github.ncitakovic.raftkv.api.v1.GetResponse;
import io.github.ncitakovic.raftkv.api.v1.KeyValueGrpc;
import io.github.ncitakovic.raftkv.api.v1.PutRequest;
import io.github.ncitakovic.raftkv.api.v1.PutResponse;
import io.github.ncitakovic.raftkv.store.DurableKvStore;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** gRPC front end: validates requests and maps store results and errors to gRPC status codes. */
public final class KeyValueService extends KeyValueGrpc.KeyValueImplBase {

    private static final Logger log = LoggerFactory.getLogger(KeyValueService.class);
    static final int MAX_KEY_BYTES = 1024;
    static final int MAX_VALUE_BYTES = 1024 * 1024;

    private final DurableKvStore store;

    public KeyValueService(DurableKvStore store) {
        this.store = store;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responses) {
        try {
            validateKey(request.getKey());
            if (request.getValue().size() > MAX_VALUE_BYTES) {
                throw invalid("value exceeds " + MAX_VALUE_BYTES + " bytes");
            }
            DurableKvStore.WriteResult result = store.put(request.getKey(), request.getValue().toByteArray());
            responses.onNext(PutResponse.newBuilder().setIndex(result.index()).build());
            responses.onCompleted();
        } catch (StatusException e) {
            responses.onError(e);
        } catch (IOException e) {
            responses.onError(storageFailure(e));
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responses) {
        try {
            validateKey(request.getKey());
            Optional<byte[]> value = store.get(request.getKey());
            GetResponse.Builder response = GetResponse.newBuilder().setFound(value.isPresent());
            value.ifPresent(v -> response.setValue(ByteString.copyFrom(v)));
            responses.onNext(response.build());
            responses.onCompleted();
        } catch (StatusException e) {
            responses.onError(e);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responses) {
        try {
            validateKey(request.getKey());
            DurableKvStore.WriteResult result = store.delete(request.getKey());
            responses.onNext(DeleteResponse.newBuilder()
                    .setExisted(result.existed())
                    .setIndex(result.index())
                    .build());
            responses.onCompleted();
        } catch (StatusException e) {
            responses.onError(e);
        } catch (IOException e) {
            responses.onError(storageFailure(e));
        }
    }

    private static void validateKey(String key) throws StatusException {
        if (key.isEmpty()) {
            throw invalid("key must not be empty");
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > MAX_KEY_BYTES) {
            throw invalid("key exceeds " + MAX_KEY_BYTES + " bytes");
        }
    }

    private static StatusException invalid(String message) {
        return Status.INVALID_ARGUMENT.withDescription(message).asException();
    }

    private static StatusException storageFailure(IOException e) {
        // Do not leak file paths or internals to clients; log the details server-side.
        log.error("Storage failure", e);
        return Status.UNAVAILABLE.withDescription("storage failure, retry later").asException();
    }
}
