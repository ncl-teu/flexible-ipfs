package org.ncl.kadrtt.core.cmds;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.multiaddr.MultiAddress;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.peergos.HostBuilder;
import org.peergos.HttpProxyService;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.client.RequestSender;
import org.peergos.net.HttpProxyHandler;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.http.HttpProtocol;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CmdMain {
    public static void main(String[] args) {

        String str = "/ip4/35.78.143.45/tcp/4001";

        String regex = "((([01]?\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(([01]?\\d{1,2})|(2[0-4]\\d)|(25[0-5]))";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(str);
        if (m.find()) {
            System.out.println(m.group());
        }
        /*NabuClient client = new NabuClient(new MultiAddress("/ip4/127.0.0.1/tcp/5001"));
        try{
            System.out.println("version:"+client.version());
            String val = "Hello World";
            List<byte[]> list = new LinkedList<byte[]>();
            list.add(val.getBytes());
            //client.putBlocks(list, Optional.empty());
            //client.find

        }catch(Exception e){
            e.printStackTrace();
        }*/
        /*
        HttpProtocol.Binding node1Http = new HttpProtocol.Binding((s, req, h) -> {
            FullHttpResponse emptyReply = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.buffer(0));
            emptyReply.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
            h.accept(emptyReply.retain());
        });
        HostBuilder builder1 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), new RamBlockstore(), (c, b, p, a) -> CompletableFuture.completedFuture(true));
        builder1 = builder1.addProtocol(node1Http);
        Host node1 = builder1.build();
        node1.start().join();

        String responseText = "nabu!";
        String requestBody = "request body!";
        Map<String, String> requestHeaders = new HashMap<>();
        String testHeaderKey = "testProp";
        String testHeaderValue = "testPropValue";
        requestHeaders.put(testHeaderKey, testHeaderValue);
        String urlParamKey = "text";
        String urlParamValue = "hello";
        String urlParam = "?" + urlParamKey + "=" + urlParamValue + "";
        RamBlockstore blockstore2 = new RamBlockstore();
        int localPort = 8321;

        HttpProtocol.Binding node2Http = new HttpProtocol.Binding((s, req, h) -> {
            System.out.println("Node 2 received: " + req);
            printBody(req);
            FullHttpRequest fullRequest = (FullHttpRequest)req;
            String uri = req.uri();
            QueryStringDecoder queryStringDecoder = new QueryStringDecoder(uri);
            Map<String, List<String>> params = queryStringDecoder.parameters();
            String paramValue = params.get(urlParamKey).get(0);
            HttpHeaders headers = req.headers();
            String headerValue = headers.get(testHeaderKey);

            ByteBuf content = fullRequest.content();
            String bodyContent = content.toString(CharsetUtil.UTF_8);
            MultiAddress proxyTargetAddress = new MultiAddress("/ip4/127.0.0.1/tcp/" + localPort);
            try {
                FullHttpResponse reply = RequestSender.proxy(proxyTargetAddress, (FullHttpRequest) req);
                h.accept(reply.retain());
            } catch (IOException ioe) {
            }
        });
        HostBuilder builder2 = HostBuilder.create(10000 + new Random().nextInt(50000),
                new RamProviderStore(), new RamRecordStore(), blockstore2, (c, b, p, a) -> CompletableFuture.completedFuture(true));
        builder2 = builder2.addProtocol(node2Http);
        Host node2 = builder2.build();
        node2.start().join();

        Multiaddr node2Address = node2.listenAddresses().get(0);
        PeerId peerId2 = node2Address.getPeerId();

        node1.getAddressBook().setAddrs(peerId2, 0, node2Address).join();

        HttpServer apiServer1 = null;
        HttpServer server2 = null;
        try {
            int port = 8777;
            MultiAddress apiAddress1 = new MultiAddress("/ip4/127.0.0.1/tcp/" + port);
            InetSocketAddress localAPIAddress1 = new InetSocketAddress(apiAddress1.getHost(), apiAddress1.getPort());
            apiServer1 = HttpServer.create(localAPIAddress1, 500);
            apiServer1.createContext(HttpProxyService.API_URL, new HttpProxyHandler(new HttpProxyService(node1, node1Http, null)));
            apiServer1.setExecutor(Executors.newFixedThreadPool(50));
            apiServer1.start();

            InetSocketAddress proxyTarget = new InetSocketAddress("127.0.0.1", localPort);
            server2 = HttpServer.create(proxyTarget, 20);
            server2.createContext("/", httpExchange -> {
                try {
                    byte[] body = responseText.getBytes();
                    httpExchange.sendResponseHeaders(200, body.length);
                    httpExchange.getResponseBody().write(body);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    httpExchange.close();
                }
            });
            server2.setExecutor(Executors.newSingleThreadExecutor());
            server2.start();

            URL target = new URL("http", "localhost", port,
                    "/p2p/" + peerId2.toBase58() + "/http/message" + urlParam);
            RequestSender.Response reply = RequestSender.send(target, "POST", requestBody.getBytes(), requestHeaders);
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally {
            node1.stop();
            node2.stop();
            if (apiServer1 != null) {
                apiServer1.stop(1);
            }
            if (server2 != null) {
                server2.stop(1);
            }
        }

    }

    public static void printBody(HttpRequest req) {
        if (req instanceof FullHttpRequest) {
            ByteBuf content = ((FullHttpRequest) req).content();
            System.out.println(content.getCharSequence(0, content.readableBytes(), Charset.defaultCharset()));
        }

    }*/
    }
}
