package io.github.ncitakovic.raftkv.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.github.ncitakovic.raftkv.api.v1.DeleteRequest;
import io.github.ncitakovic.raftkv.api.v1.GetRequest;
import io.github.ncitakovic.raftkv.api.v1.GetResponse;
import io.github.ncitakovic.raftkv.api.v1.KeyValueGrpc;
import io.github.ncitakovic.raftkv.api.v1.PutRequest;
import io.github.ncitakovic.raftkv.store.DurableKvStore;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class KeyValueServiceTest {

    @TempDir
    Path dir;

    private DurableKvStore store;
    private Server server;
    private ManagedChannel channel;
    private KeyValueGrpc.KeyValueBlockingStub client;

    @BeforeEach
    void setUp() throws Exception {
        store = DurableKvStore.open(dir);
        String name = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(name)
                .directExecutor()
                .addService(new KeyValueService(store))
                .build()
                .start();
        channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        client = KeyValueGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() throws Exception {
        channel.shutdownNow();
        server.shutdownNow();
        store.close();
    }

    @Test
    void putGetDeleteRoundTrip() {
        long index = client.put(PutRequest.newBuilder()
                .setKey("user:1").setValue(ByteString.copyFromUtf8("Nikola")).build()).getIndex();
        assertEquals(1, index);

        GetResponse get = client.get(GetRequest.newBuilder().setKey("user:1").build());
        assertTrue(get.getFound());
        assertEquals("Nikola", get.getValue().toStringUtf8());

        assertTrue(client.delete(DeleteRequest.newBuilder().setKey("user:1").build()).getExisted());
        assertFalse(client.get(GetRequest.newBuilder().setKey("user:1").build()).getFound());
    }

    @Test
    void missingKeyIsNotFoundRatherThanAnError() {
        GetResponse get = client.get(GetRequest.newBuilder().setKey("nope").build());
        assertFalse(get.getFound());
        assertTrue(get.getValue().isEmpty());
    }

    @Test
    void emptyKeyIsRejected() {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                () -> client.put(PutRequest.newBuilder().setKey("").build()));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void oversizedKeyIsRejected() {
        String key = "k".repeat(KeyValueService.MAX_KEY_BYTES + 1);
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
                () -> client.get(GetRequest.newBuilder().setKey(key).build()));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }
}
