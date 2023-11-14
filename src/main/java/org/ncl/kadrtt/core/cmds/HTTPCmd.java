package org.ncl.kadrtt.core.cmds;

import io.ipfs.multiaddr.MultiAddress;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.client.RequestSender;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class HTTPCmd {
    public static void main(String[] args) {



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


        try {
            int port = 5001;
            MultiAddress apiAddress1 = new MultiAddress("/ip4/127.0.0.1/tcp/" + port);
            InetSocketAddress localAPIAddress1 = new InetSocketAddress(apiAddress1.getHost(), apiAddress1.getPort());

            URL target = new URL("http", "localhost", port,
                    "/p2p/" + "12D3KooWA5pL6SFauCsKqJK7J8dzXSUGqNbSQLNQ7PkaDihvv1ZG" + "/http/message" + urlParam);
            RequestSender.Response reply = RequestSender.send(target, "POST", requestBody.getBytes(), requestHeaders);
            System.out.println();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }


    }

    public static void printBody(HttpRequest req) {
        if (req instanceof FullHttpRequest) {
            ByteBuf content = ((FullHttpRequest) req).content();
            System.out.println(content.getCharSequence(0, content.readableBytes(), Charset.defaultCharset()));
        }

    }
}
