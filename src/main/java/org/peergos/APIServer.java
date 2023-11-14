package org.peergos;

import com.sun.net.httpserver.HttpServer;
import io.ipfs.multiaddr.MultiAddress;
import io.netty.handler.codec.http.*;
import org.ncl.kadrtt.core.Kad;
import org.peergos.client.*;
import org.peergos.config.*;
import org.peergos.net.APIHandler;
import org.peergos.net.HttpProxyHandler;
import org.peergos.protocol.http.*;
import org.peergos.util.HttpUtil;
import org.peergos.util.Logging;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.peergos.EmbeddedIpfs.buildBlockStore;

public class APIServer {

    private static final Logger LOG = Logger.getLogger(APIServer.class.getName());

    private static HttpProtocol.HttpRequestProcessor proxyHandler(MultiAddress target) {
        return (s, req, h) -> {
            try {
                FullHttpResponse reply = RequestSender.proxy(target, (FullHttpRequest) req);
                h.accept(reply.retain());
            } catch (IOException ioe) {
                FullHttpResponse exceptionReply = HttpUtil.replyError(ioe);
                h.accept(exceptionReply.retain());
            }
        };
    }

    public APIServer(Args args) throws Exception {



        //.ipfsを取得する．
        Path ipfsPath = getIPFSPath(args);
        Logging.init(ipfsPath, args.getBoolean("logToConsole", false));
        //configを取得する．
        Config config = readConfig(ipfsPath);

        LOG.info("Starting Nabu version: " + APIHandler.CURRENT_VERSION);
        BlockRequestAuthoriser authoriser = (c, b, p, a) -> CompletableFuture.completedFuture(true);

        Kad.getIns().initialize("kadrtt.properties");
        //ipfsを生成
        //ここでHTTPサーバプロセスを立ち上げる？
        EmbeddedIpfs ipfs = EmbeddedIpfs.build(ipfsPath,
                buildBlockStore(config, ipfsPath),
                config.addresses.getSwarmAddresses(),
                config.bootstrap.getBootstrapAddresses(),
                config.identity,
                authoriser,
                config.addresses.proxyTargetAddress.map(APIServer::proxyHandler)
        );
        ipfs.start();
        String apiAddressArg = "Addresses.API";
        MultiAddress apiAddress = args.hasArg(apiAddressArg) ? new MultiAddress(args.getArg(apiAddressArg)) : config.addresses.apiAddress;
        InetSocketAddress localAPIAddress = new InetSocketAddress(apiAddress.getHost(), apiAddress.getPort());

        int maxConnectionQueue = 500;
        int handlerThreads = 50;
        LOG.info("Starting RPC API server at " + apiAddress.getHost() + ":" + localAPIAddress.getPort());
        HttpServer apiServer = HttpServer.create(localAPIAddress, maxConnectionQueue);

        apiServer.createContext(APIHandler.API_URL, new APIHandler(ipfs));
        //HttpProxyHandlerが，リクエストを受け付ける．
        if (config.addresses.proxyTargetAddress.isPresent())
            apiServer.createContext(HttpProxyService.API_URL, new HttpProxyHandler(new HttpProxyService(ipfs.node, ipfs.p2pHttp.get(), ipfs.dht)));
        apiServer.setExecutor(Executors.newFixedThreadPool(handlerThreads));
        apiServer.start();


        Thread shutdownHook = new Thread(() -> {
            LOG.info("Stopping server...");
            try {
                ipfs.stop().join();
                apiServer.stop(3); //wait max 3 seconds
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("Input IPFS Command:>");
            String old = scanner.next();

            System.out.println("input value:" + old);
        }
    }

    private Path getIPFSPath(Args args) {
        Optional<String> ipfsPath = args.getOptionalArg("IPFS_PATH");
        if (ipfsPath.isEmpty()) {
            String home = args.getArg("HOME");
            return Path.of(home, ".ipfs");
        }
        return Path.of(ipfsPath.get());
    }

    private Config readConfig(Path configPath) throws IOException {
        Path configFilePath = configPath.resolve("config");
        File configFile = configFilePath.toFile();
        if (!configFile.exists()) {
            LOG.info("Unable to find config file. Creating default config");
            Config config = new Config();
            Files.write(configFilePath, config.toString().getBytes(), StandardOpenOption.CREATE);
            return config;
        }
        return Config.build(Files.readString(configFilePath));
    }

    public static void main(String[] args) {
        try {
            new APIServer(Args.parse(args));


        } catch (Exception e) {
            LOG.log(Level.SEVERE, "SHUTDOWN", e);
        }
    }
}