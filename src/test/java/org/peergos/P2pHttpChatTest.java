package org.peergos;

import io.libp2p.core.*;
import io.libp2p.core.multiformats.*;
import io.netty.buffer.*;
import io.netty.handler.codec.http.*;
import org.junit.*;
import org.peergos.blockstore.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;

import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.*;

public class P2pHttpChatTest {

    @Test
    public void p2pHttpChat() {
        FullHttpResponse replyOk = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.buffer(0));
        replyOk.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
        HttpProtocol.Binding node1Http = new HttpProtocol.Binding((s, req, h) -> {
            System.out.println("Node 1 received: " + req);
            printBody(req);
            h.accept(replyOk.retain());
        });
        HostBuilder builder1 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(node1Http);
        Host node1 = builder1.build();
        HttpProtocol.Binding node2Http = new HttpProtocol.Binding((s, req, h) -> {
            System.out.println("Node 2 received: " + req);
            printBody(req);
            h.accept(replyOk);
        });
        HostBuilder builder2 = HostBuilder.create(10000 + new Random().nextInt(50000),
                        new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true))
                .addProtocol(node2Http);
        Host node2 = builder2.build();
        node1.start().join();
        node2.start().join();

        try {
            Multiaddr address1 = node1.listenAddresses().get(0);
            Multiaddr address2 = node2.listenAddresses().get(0);

            int count = 200;
            long totalDuration = 0;
            for (int i = 0; i < count; i++) {
                byte[] msg1 = "G'day from node1!".getBytes();
                FullHttpRequest httpRequest = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.copiedBuffer(msg1));
                httpRequest.headers().set(HttpHeaderNames.CONTENT_LENGTH, msg1.length);
                HttpProtocol.HttpController proxier1 = node1Http.dial(node1, address2)
                        .getController().join();
                long t1 = System.currentTimeMillis();
                proxier1.send(httpRequest.retain()).join();
                long t2 = System.currentTimeMillis();
                System.out.println("P2P HTTP request took " + (t2 - t1) + "ms");
                totalDuration += t2 - t1;

                byte[] msg2 = "G'day node1! I'm node2.".getBytes();
                FullHttpRequest httpRequest2 = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", Unpooled.copiedBuffer(msg2));
                httpRequest2.headers().set(HttpHeaderNames.CONTENT_LENGTH, msg2.length);
                HttpProtocol.HttpController proxier2 = node2Http.dial(node2, address1)
                        .getController().join();
                long t3 = System.currentTimeMillis();
                proxier2.send(httpRequest2.retain()).join();
                long t4 = System.currentTimeMillis();
                System.out.println("P2P HTTP request took " + (t4 - t3) + "ms");
                totalDuration += t4 - t3;
            }
            System.out.println("Average: " + (totalDuration / (count * 2)));
        } finally {
            node1.stop();
            node2.stop();
        }
    }
    public static void printBody(HttpRequest req) {
        if (req instanceof FullHttpRequest) {
            ByteBuf content = ((FullHttpRequest) req).content();
            System.out.println(content.getCharSequence(0, content.readableBytes(), Charset.defaultCharset()));
        }

    }
}
