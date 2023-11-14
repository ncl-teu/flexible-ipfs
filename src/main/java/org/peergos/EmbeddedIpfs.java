package org.peergos;

import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.*;
import io.libp2p.core.*;
import io.libp2p.core.multistream.*;
import io.libp2p.protocol.*;
import org.ncl.kadrtt.core.Kad;
import org.peergos.blockstore.*;
import org.peergos.config.*;
import org.peergos.protocol.autonat.*;
import org.peergos.protocol.bitswap.*;
import org.peergos.protocol.circuit.*;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.http.*;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Supplier;
import java.util.logging.*;
import java.util.stream.*;

public class EmbeddedIpfs {
    private static final Logger LOG = Logger.getLogger(EmbeddedIpfs.class.getName());

    public final Host node;
    public final ProvidingBlockstore blockstore;
    public final BlockService blocks;
    public final DatabaseRecordStore records;

    public final Kademlia dht;
    public final Bitswap bitswap;
    public final Optional<HttpProtocol.Binding> p2pHttp;
    private final List<MultiAddress> bootstrap;

    public EmbeddedIpfs(Host node,
                        ProvidingBlockstore blockstore,
                        DatabaseRecordStore records,
                        Kademlia dht,
                        Bitswap bitswap,
                        Optional<HttpProtocol.Binding> p2pHttp,
                        List<MultiAddress> bootstrap) {
        this.node = node;
        Kad.getIns().setNode(this.node);
        String globalIP = Kad.getIns().getGlobalIP();
        MultiAddress mAddr = new MultiAddress("/ip4/"+globalIP + "/tcp/4001");
        List<MultiAddress> addrList = new LinkedList<MultiAddress>();
        addrList.add(mAddr);
        Multihash deHash = Multihash.deserialize(this.node.getPeerId().getBytes());
        PeerAddresses addrs = new PeerAddresses(deHash,addrList );
        Kad.getIns().setOwnAddresses(addrs);

        this.blockstore = blockstore;
        this.records = records;
        this.dht = dht;
        this.bitswap = bitswap;
        this.p2pHttp = p2pHttp;
        this.bootstrap = bootstrap;
        this.blocks = new BitswapBlockService(node, bitswap);



    }

    public List<HashedBlock> getBlocks(List<Want> wants, Set<PeerId> peers, boolean addToLocal) {
        List<HashedBlock> blocksFound = new ArrayList<>();

        List<Want> local = new ArrayList<>();
        List<Want> remote = new ArrayList<>();

        for (Want w : wants) {
            if (blockstore.has(w.cid).join())
                local.add(w);
            else
                remote.add(w);
        }
        local.stream()
                .map(w -> new HashedBlock(w.cid, blockstore.get(w.cid).join().get()))
                .forEach(blocksFound::add);
        if (remote.isEmpty())
            return blocksFound;
        return java.util.stream.Stream.concat(
                        blocksFound.stream(),
                        blocks.get(remote, peers, addToLocal).stream())
                .collect(Collectors.toList());
    }

    public void start() {
        //node.start().join();

        node.start();
        try{
            Thread.sleep(3000);
        }catch(Exception e){
            e.printStackTrace();
        }
        //node.join();
        //LOG.info("Node started and listening on " + node.listenAddresses());
        LOG.info("Starting bootstrap process");
        int connections = dht.bootstrapRoutingTable(node, bootstrap, addr -> !addr.contains("/wss/"));
       // int connections = dht.bootstrapRoutingTable(node, bootstrap, addr -> addr.contains("35.78.143.45"));
        if (connections == 0)
            throw new IllegalStateException("No connected peers!");
        dht.bootstrap(node);

        PeriodicBlockProvider blockProvider = new PeriodicBlockProvider(22 * 3600_000L,
                () -> blockstore.refs().join().stream(), node, dht, blockstore.toPublish);
        blockProvider.start();
    }

    public CompletableFuture<Void> stop() throws Exception {
        records.close();
        return node.stop();
    }

    public static Blockstore buildBlockStore(Config config, Path ipfsPath) {
        FileBlockstore blocks = new FileBlockstore(ipfsPath);
        Blockstore blockStore;
        if (config.datastore.filter.type == FilterType.BLOOM) {
            blockStore = FilteredBlockstore.bloomBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if(config.datastore.filter.type == FilterType.INFINI) {
            blockStore = FilteredBlockstore.infiniBased(blocks, config.datastore.filter.falsePositiveRate);
        } else if(config.datastore.filter.type == FilterType.NONE) {
            blockStore = blocks;
        } else {
            throw new IllegalStateException("Unhandled filter type: " + config.datastore.filter.type);
        }
        return config.datastore.allowedCodecs.codecs.isEmpty() ?
                blockStore : new TypeLimitedBlockstore(blockStore, config.datastore.allowedCodecs.codecs);
    }

    public static EmbeddedIpfs build(Path ipfsPath,
                                     Blockstore blocks,
                                     List<MultiAddress> swarmAddresses,
                                     List<MultiAddress> bootstrap,
                                     IdentitySection identity,
                                     BlockRequestAuthoriser authoriser,
                                     Optional<HttpProtocol.HttpRequestProcessor> handler) {
        ProvidingBlockstore blockstore = new ProvidingBlockstore(blocks);
        Path datastorePath = ipfsPath.resolve("datastore").resolve("h2.datastore");
        DatabaseRecordStore records = new DatabaseRecordStore(datastorePath.toString());
        ProviderStore providers = new RamProviderStore();
        Kad.getIns().setProviderStore(providers);
        Kad.getIns().setStore(records);


        HostBuilder builder = new HostBuilder().setIdentity(identity.privKeyProtobuf).listen(swarmAddresses);
        if (! builder.getPeerId().equals(identity.peerId)) {
            throw new IllegalStateException("PeerId invalid");
        }
        Multihash ourPeerId = Multihash.deserialize(builder.getPeerId().getBytes());

        Kademlia dht = new Kademlia(new KademliaEngine(ourPeerId, providers, records), false);
        Kad.getIns().setKadDHT(dht);
        CircuitStopProtocol.Binding stop = new CircuitStopProtocol.Binding();
        CircuitHopProtocol.RelayManager relayManager = CircuitHopProtocol.RelayManager.limitTo(builder.getPrivateKey(), ourPeerId, 5);
        Bitswap bitswap = new Bitswap(new BitswapEngine(blockstore, authoriser));
        Optional<HttpProtocol.Binding> httpHandler = handler.map(HttpProtocol.Binding::new);

        List<ProtocolBinding> protocols = new ArrayList<>();
        protocols.add(new Ping());
        protocols.add(new AutonatProtocol.Binding());
        protocols.add(new CircuitHopProtocol.Binding(relayManager, stop));
        protocols.add(bitswap);
        protocols.add(dht);
        httpHandler.ifPresent(protocols::add);

        Host node = builder.addProtocols(protocols).build();


        return new EmbeddedIpfs(node, blockstore, records, dht, bitswap, httpHandler, bootstrap);
    }
}
