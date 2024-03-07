package org.ncl.kadrtt.core.cmds;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import org.ncl.kadrtt.core.Kad;
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

        String cnt = "content";

        for(int i=1;i<301;i++){
            String content = cnt + i;
            Cid cid = Kad.genCid(content);
            System.out.println(cid);

        }

/*
        String str = "/ip4/35.78.143.45/tcp/4001";

        String regex = "((([01]?\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(([01]?\\d{1,2})|(2[0-4]\\d)|(25[0-5]))";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(str);
        if (m.find()) {
            System.out.println(m.group());
        }

 */

    }
}
