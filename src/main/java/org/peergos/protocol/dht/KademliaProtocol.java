package org.peergos.protocol.dht;

import io.libp2p.core.*;
import io.libp2p.core.mux.StreamMuxer;
import io.libp2p.core.transport.Transport;
import io.libp2p.protocol.*;
import org.jetbrains.annotations.*;
import org.peergos.protocol.dht.pb.Dht;

import java.util.concurrent.*;

public class KademliaProtocol extends ProtobufProtocolHandler<KademliaController> {
    public static final int MAX_MESSAGE_SIZE = 1024*1024;

    private final KademliaEngine engine;

    /**
     * Private IPFS用のswarmKey
     */
    private String swarmKey;

    public KademliaProtocol(KademliaEngine engine) {
        super(Dht.Message.getDefaultInstance(), MAX_MESSAGE_SIZE, MAX_MESSAGE_SIZE);
        this.engine = engine;
        this.swarmKey = null;
    }


    public String getSwarmKey() {
        return swarmKey;
    }

    public void setSwarmKey(String swarmKey) {
        this.swarmKey = swarmKey;
    }


    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartInitiator(@NotNull Stream stream) {
        engine.addOutgoingConnection(stream.remotePeerId(), stream.getConnection().remoteAddress());
        //engine.addOutgoingConnection(stream.remotePeerId(), stream.getConnection().localAddress());
        StreamMuxer.Session ses = stream.getConnection().muxerSession();
        Transport tran = stream.getConnection().transport();

        ReplyHandler handler = new ReplyHandler(stream);
        stream.pushHandler(handler);
        return CompletableFuture.completedFuture(handler);

    }

    @NotNull
    @Override
    protected CompletableFuture<KademliaController> onStartResponder(@NotNull Stream stream) {
        engine.addIncomingConnection(stream.remotePeerId(), stream.getConnection().remoteAddress());
        //Add by Kanemitsu
        //engine.addIncomingConnection(stream.remotePeerId(), stream.getConnection().localAddress());
        StreamMuxer.Session ses = stream.getConnection().muxerSession();
        Transport tran = stream.getConnection().transport();


        IncomingRequestHandler handler = new IncomingRequestHandler(engine);
        stream.pushHandler(handler);
        return CompletableFuture.completedFuture(handler);
    }

    class ReplyHandler implements ProtocolMessageHandler<Dht.Message>, KademliaController {
        private final CompletableFuture<Dht.Message> resp = new CompletableFuture<>();
        private final Stream stream;

        public ReplyHandler(Stream stream) {
            this.stream = stream;
        }

        @Override
        public CompletableFuture<Dht.Message> rpc(Dht.Message msg) {
            stream.writeAndFlush(msg);
            return resp;
        }

        @Override
        public CompletableFuture<Boolean> send(Dht.Message msg) {
            stream.writeAndFlush(msg);
            return CompletableFuture.completedFuture(true);
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            resp.complete(msg);
            stream.closeWrite();
        }

        @Override
        public void onClosed(@NotNull Stream stream) {
            resp.completeExceptionally(new ConnectionClosedException());
        }

        @Override
        public void onException(@Nullable Throwable cause) {
            resp.completeExceptionally(cause);
        }
    }

    class IncomingRequestHandler implements ProtocolMessageHandler<Dht.Message>, KademliaController {
        private final KademliaEngine engine;

        public IncomingRequestHandler(KademliaEngine engine) {
            this.engine = engine;
        }

        @Override
        public void onMessage(@NotNull Stream stream, Dht.Message msg) {
            engine.receiveRequest(msg, stream.remotePeerId(), stream);
        }

        @Override
        public CompletableFuture<Dht.Message> rpc(Dht.Message msg) {
            throw new IllegalStateException("Responder only!");
        }

        @Override
        public CompletableFuture<Boolean> send(Dht.Message msg) {
            throw new IllegalStateException("Responder only!");
        }
    }
}
