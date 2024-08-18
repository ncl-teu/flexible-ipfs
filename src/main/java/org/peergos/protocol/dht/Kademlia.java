package org.peergos.protocol.dht;

import com.google.protobuf.ByteString;
import com.offbynull.kademlia.Id;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.crypto.PrivKey;
import io.libp2p.core.multiformats.Multiaddr;
import io.libp2p.core.multistream.StrictProtocolBinding;
import io.libp2p.etc.types.NothingToCompleteException;
import io.libp2p.protocol.Identify;
import org.ncl.kadrtt.core.Kad;
import org.ncl.kadrtt.core.cmds.ChunkPutThread;
import org.peergos.AddressBookConsumer;
import org.peergos.Hash;
import org.peergos.PeerAddresses;
import org.peergos.Providers;
import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;
import org.peergos.protocol.dht.pb.Dht;
import org.peergos.protocol.dnsaddr.DnsAddr;
import org.peergos.protocol.ipns.GetResult;
import org.peergos.protocol.ipns.IPNS;
import org.peergos.protocol.ipns.IpnsRecord;
import org.peergos.protocol.ipns.pb.Ipns;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Kademlia extends StrictProtocolBinding<KademliaController> implements AddressBookConsumer {

    private static final Logger LOG = Logger.getLogger(Kademlia.class.getName());
    public static final int BOOTSTRAP_PERIOD_MILLIS = 300_000;
    private final KademliaEngine engine;
    private final boolean localDht;
    private AddressBook addressBook;

    public Kademlia(KademliaEngine dht, boolean localOnly) {
        super("/ipfs/" + (localOnly ? "lan/" : "") + "kad/1.0.0", new KademliaProtocol(dht));
        this.engine = dht;
        this.localDht = localOnly;
    }

    public void setAddressBook(AddressBook addrs) {
        engine.setAddressBook(addrs);
        this.addressBook = addrs;
    }

    public int bootstrapRoutingTable(Host host, List<MultiAddress> addrs, Predicate<String> filter) {
        List<String> resolved = addrs.stream()
                .parallel()
                .flatMap(a -> {
                    try {
                        return DnsAddr.resolve(a.toString()).stream();
                    } catch (CompletionException ce) {
                        return Stream.empty();
                    }
                })
                .filter(filter)
                .collect(Collectors.toList());
        List<? extends CompletableFuture<? extends KademliaController>> futures = resolved.stream()
                .parallel()
                .map(addr -> dial(host, Multiaddr.fromString(addr)).getController())
                .collect(Collectors.toList());
        int successes = 0;
        for (CompletableFuture<? extends KademliaController> future : futures) {
            try {
                future.orTimeout(5, TimeUnit.SECONDS).join();
                successes++;
            } catch (Exception e) {}
        }
        return successes;
    }

    public void startBootstrapThread(Host us) {
        new Thread(() -> {
            while (true) {
                try {
                    bootstrap(us);
                    Thread.sleep(BOOTSTRAP_PERIOD_MILLIS);
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }, "Kademlia bootstrap").start();
    }

    private boolean connectTo(Host us, PeerAddresses peer) {
        try {
            new Identify().dial(us, PeerId.fromBase58(peer.peerId.toBase58()), getPublic(peer)).getController().join().id().join();
            return true;
        } catch (Exception e) {
            if (e.getCause() instanceof NothingToCompleteException)
                LOG.info("Couldn't connect to " + peer.peerId);
            else
                e.printStackTrace();
            return false;
        }
    }
    public void bootstrap(Host us) {
        // lookup a random peer id
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        Multihash randomPeerId = new Multihash(Multihash.Type.sha2_256, hash);
        int num = Kad.k;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        findClosestPeers(randomPeerId, num, us);
        //findClosestPeers(node1Id, num, us);


        // lookup our own peer id to keep our nearest neighbours up-to-date,
        // and connect to all of them, so they know about our addresses
        List<PeerAddresses> closestToUs = findClosestPeers(Multihash.deserialize(us.getPeerId().getBytes()), 20, us);
        int connectedClosest = 0;
        for (PeerAddresses peer : closestToUs) {
            if (connectTo(us, peer))
                connectedClosest++;
        }
        LOG.info("Bootstrap connected to " + connectedClosest + " nodes close to us.");
    }

    static class RoutingEntry {
        public final Id key;
        public final PeerAddresses addresses;

        public RoutingEntry(Id key, PeerAddresses addresses) {
            this.key = key;
            this.addresses = addresses;
        }

    }

    private int compareKeys(RoutingEntry a, RoutingEntry b, Id keyId) {
        int prefixDiff = b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId);
        if (prefixDiff != 0)
            return prefixDiff;
        return a.addresses.peerId.toBase58().compareTo(b.addresses.peerId.toBase58());
    }

    public List<PeerAddresses> findClosestPeers(Multihash peerIdkey, int maxCount, Host us) {
        byte[] key = peerIdkey.toBytes();
        Id keyId = Id.create(Hash.sha256(key), 256);
        SortedSet<RoutingEntry> closest = new TreeSet<>((a, b) -> compareKeys(a, b, keyId));
        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> compareKeys(a, b, keyId));
        List<PeerAddresses> localClosest = engine.getKClosestPeers(key);
        if (maxCount == 1) {
            Collection<Multiaddr> existing = addressBook.get(PeerId.fromBase58(peerIdkey.toBase58())).join();
            if (! existing.isEmpty())
                return Collections.singletonList(new PeerAddresses(peerIdkey, existing.stream().map(a -> a.toString()).map(MultiAddress::new).collect(Collectors.toList())));
            Optional<PeerAddresses> match = localClosest.stream().filter(p -> p.peerId.equals(peerIdkey)).findFirst();
            if (match.isPresent())
                return Collections.singletonList(match.get());
        }
        //ローカルのピアたちをリストへいれる．
        closest.addAll(localClosest.stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));
        //問い合わせ先にローカルを全ていれる．
        toQuery.addAll(closest);
        Set<Multihash> queried = new HashSet<>();
        int queryParallelism = Kad.getIns().getAlpha(0);
        while (true) {
            //queryParallelism（上位何件か）を抽出する．
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(queryParallelism).collect(Collectors.toList());
            //問い合わせ先から抽出されたピア集合を消す．
            toQuery.removeAll(queryThisRound);
            //抽出されたピア集合に対し，より近いピア集合を取得する．
            queryThisRound.forEach(r -> queried.add(r.addresses.peerId));
            List<CompletableFuture<List<PeerAddresses>>> futures = queryThisRound.stream()
                    .map(r -> getCloserPeers(peerIdkey, r.addresses, us))
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<List<PeerAddresses>> future : futures) {
                List<PeerAddresses> result = future.join();
                for (PeerAddresses peer : result) {
                    if (! queried.contains(peer.peerId)) {
                        // exit early if we are looking for the specific node
                        if (maxCount == 1 && peer.peerId.equals(peerIdkey))
                            return Collections.singletonList(peer);
                        queried.add(peer.peerId);
                        Id peerKey = Id.create(Hash.sha256(peer.peerId.toBytes()), 256);
                        RoutingEntry e = new RoutingEntry(peerKey, peer);
                        toQuery.add(e);
                        closest.add(e);
                        foundCloser = true;
                    }
                }
            }
            // if no new peers in top k were returned we are done
            if (! foundCloser)
                break;
        }
        return closest.stream()
                .limit(maxCount).map(r -> r.addresses)
                .collect(Collectors.toList());
    }

    public CompletableFuture<List<PeerAddresses>> findProviders(Multihash block, Host us, int desiredCount) {
        byte[] key = block.bareMultihash().toBytes();
        Id keyId = Id.create(key, 256);
        List<PeerAddresses> providers = new ArrayList<>();

        SortedSet<RoutingEntry> toQuery = new TreeSet<>((a, b) -> b.key.getSharedPrefixLength(keyId) - a.key.getSharedPrefixLength(keyId));
        toQuery.addAll(engine.getKClosestPeers(key).stream()
                .map(p -> new RoutingEntry(Id.create(Hash.sha256(p.peerId.toBytes()), 256), p))
                .collect(Collectors.toList()));

        Set<Multihash> queried = new HashSet<>();
        int queryParallelism = Kad.getIns().getAlpha(0);
        while (true) {
            if (providers.size() >= desiredCount)
                return CompletableFuture.completedFuture(providers);
            List<RoutingEntry> queryThisRound = toQuery.stream().limit(queryParallelism).collect(Collectors.toList());
            toQuery.removeAll(queryThisRound);
            queryThisRound.forEach(r -> queried.add(r.addresses.peerId));
            List<CompletableFuture<Providers>> futures = queryThisRound.stream()
                    .parallel()
                    .map(r -> {
                        KademliaController res = null;
                        try {
                            res = dialPeer(r.addresses, us).join();
                            return res.getProviders(block).orTimeout(5, TimeUnit.SECONDS);
                        }catch (Exception e) {
                            return null;
                        }
                    }).filter(prov -> prov != null)
                    .collect(Collectors.toList());
            boolean foundCloser = false;
            for (CompletableFuture<Providers> future : futures) {
                try {
                    Providers newProviders = future.join();
                    providers.addAll(newProviders.providers);
                    for (PeerAddresses peer : newProviders.closerPeers) {
                        if (!queried.contains(peer.peerId)) {
                            queried.add(peer.peerId);
                            RoutingEntry e = new RoutingEntry(Id.create(Hash.sha256(peer.peerId.toBytes()), 256), peer);
                            toQuery.add(e);
                            foundCloser = true;
                        }
                    }
                } catch (Exception e) {
                    if (! (e.getCause() instanceof TimeoutException))
                        e.printStackTrace();
                }
            }
            // if no new peers in top k were returned we are done
            if (! foundCloser)
                break;
        }

        return CompletableFuture.completedFuture(providers);
    }

    private CompletableFuture<List<PeerAddresses>> getCloserPeers(Multihash peerIDKey, PeerAddresses target, Host us) {
        try {
            /**
             * ここでグローバルIPでしか見てないので，LAN内のピア同士は，同じものとして扱われる．
             */

            if(target.peerId.toString().equals(us.getPeerId().toString())){

            }else{
                return dialPeer(target, us).orTimeout(2, TimeUnit.SECONDS).join().closerPeers(peerIDKey);

            }
        } catch (Exception e) {
            if (e.getCause() instanceof NothingToCompleteException)
                LOG.info("Couldn't dial " + peerIDKey + " addrs: " + target.addresses);
            else if (e.getCause() instanceof TimeoutException)
                LOG.info("Timeout dialing " + peerIDKey + " addrs: " + target.addresses);
            else
                e.printStackTrace();
        }
        return CompletableFuture.completedFuture(Collections.emptyList());
    }

    private Multiaddr[] getPublic(PeerAddresses target) {
        return target.addresses.stream()
                .filter(a -> localDht || a.isPublic(false))
                .map(a -> Multiaddr.fromString(a.toString()))
                .collect(Collectors.toList()).toArray(new Multiaddr[0]);
    }

    public CompletableFuture<? extends KademliaController> dialPeer(PeerAddresses target, Host us) {
       Multiaddr[] multiaddrs = getPublic(target);

        MultiAddress addr = target.addresses.get(0);
        //なぜか，4001宛にしか接続が受け入れられないので，targetのportが4001以外の場合は，強制的に4001にする．
        if(addr.getPort() != 4001){
            String[] parts = addr.toString().substring(1).split("/");
            parts[3] = "4001";
            StringBuffer buf = new StringBuffer("/");
            for(int i=0;i<parts.length;i++){
                buf.append(parts[i]);
                buf.append("/");

            }
            //MultiAddress newAddr = new MultiAddress(buf.toString());
            Multiaddr newAddr = new Multiaddr(buf.toString());

            if(multiaddrs.length > 0){
                multiaddrs[0] = newAddr;

            }else{
                multiaddrs = new Multiaddr[1];
                multiaddrs[0] = newAddr;
            }
        }

        return dial(us, PeerId.fromBase58(target.peerId.toBase58()), multiaddrs).getController();

    }


    public CompletableFuture<Void> provideBlock(Multihash block, Host us, PeerAddresses ourAddrs) {
        List<PeerAddresses> closestPeers = findClosestPeers(block, Kad.getIns().getBeta(), us);
        List<CompletableFuture<Boolean>> provides = closestPeers.stream()
                .parallel()
                .map(p -> dialPeer(p, us).join().provide(block, ourAddrs))
                .collect(Collectors.toList());
        return CompletableFuture.allOf(provides.toArray(new CompletableFuture[0]));
    }

    public CompletableFuture<Void> publishIpnsValue(PrivKey priv, Multihash publisher, Multihash value, long sequence, Host us) {
        int hours = 1;
        LocalDateTime expiry = LocalDateTime.now().plusHours(hours);
        long ttl = hours * 3600_000_000_000L;

        int publishes = 0;
        while (publishes < 20) {
            List<PeerAddresses> closestPeers = findClosestPeers(publisher, Kad.getIns().getBeta(), us);
            for (PeerAddresses peer : closestPeers) {
                boolean success = dialPeer(peer, us).join().putValue("/ipfs/" + value, expiry, sequence,
                        ttl, publisher, priv).join();
                if (success)
                    publishes++;
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    public CompletableFuture<String> resolveIpnsValue(Multihash publisher, Host us) {
        List<PeerAddresses> closestPeers = findClosestPeers(publisher, Kad.getIns().getBeta(), us);
        List<IpnsRecord> candidates = new ArrayList<>();
        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();
        for (PeerAddresses peer : closestPeers) {
            if (queriedPeers.contains(peer.peerId))
                continue;
            queriedPeers.add(peer.peerId);
            GetResult res = dialPeer(peer, us).join().getValue(publisher).join();
            if (res.record.isPresent() && res.record.get().publisher.equals(publisher))
                candidates.add(res.record.get().value);
            queryCandidates.addAll(res.closerPeers);
        }

        // Validate and sort records by sequence number
        List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        return CompletableFuture.completedFuture(records.get(records.size() - 1).value);
    }


    /**
     * Addプロセス用のput処理です．他ピアにputしつつ，自身ノードには必ずファイルを書き込みます．
     *
     * @param chunkList
     * @param cid
     * @param us
     * @param cidList
     * @return
     */
    public List<PeerAddresses> putRawContent(boolean isAdd, LinkedList<byte[]> chunkList, Multihash cid,
                                             Host us, LinkedList<Cid> cidList, String ex){
        //最初の要素を取得する．
        byte[] data = chunkList.getFirst();
        //1ピアのみ取得する．
        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, Kad.getIns().getPutRedundancy(), us);
        PeerAddresses closestPeerID = closestPeers.get(0);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        String pathToPublish = "/ipfs/" + cid;

        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        boolean isWritten = false;

        //あとは自身のディレクトリに書き込み処理をする．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForAdd(cid.toBytes(), pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl, cidList, ex);
        CborObject cbor = CborObject.fromByteArray(cborEntryData);
        CborObject.CborMap map = (CborObject.CborMap) cbor;
        String str_cid = cid.toString();
        //System.out.println("cid:"+str_cid);
        //Addモードの場合は，DAGファイル書き込み
        if(isAdd){
            Kad.writeMerkleDAGOnly(str_cid, map);

        }

        // Kad.java.writeData
        //MerkleDAG部分のみをputするためのループ
        while (pIte.hasNext()) {
            PeerAddresses addr = pIte.next();
            if (addr.peerId.toString().equals(node1Id.toString())) {
                //continue;
                retList.add(addr);
                // isWritten = true;
            } else {
                try{
                    boolean success = dialPeer(addr, us).orTimeout(5, TimeUnit.SECONDS).join().putValueForAdd(map.toByteArray(), cidList, "/ipfs/" + cid, expiry, sequence,
                            ttl, cid, us.getPrivKey(), ex).join();

                    retList.add(addr);
                }catch(Exception e){
                    e.printStackTrace();
                }

           }

        }

        //以降は，2こめのchunkたちに対する処理．
        //chunkのputを非同期に行う．
        if(chunkList.size() >= 1){
            //すでにput済みの最初の要素を削除する．そして，それ以降については
            //chunkのみのputとする．これを非同期で行う．
           /* chunkList.removeFirst();
            Thread t = new Thread(new ChunkPutThread(this, chunkList, us));
            t.start();*/
           // Kad.java.chunkExec.submit(new ChunkPutThread(this, chunkList, us));
            Iterator<byte[]> cIte = chunkList.iterator();
            while(cIte.hasNext()){
                byte[] chunk = cIte.next();
                Cid ccid = Kad.genCid(chunk);
                Kad.chunkExec.submit(new ChunkPutThread(isAdd, this, chunk, ccid, us,ex));


            }


        }

        return retList;

    }


    public List<PeerAddresses> putRawContent(byte[] data, Multihash cid, Host us){
        //1ピアのみ取得する．
        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, Kad.getIns().getPutRedundancy(), us);
        PeerAddresses closestPeerID = closestPeers.get(0);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        String pathToPublish = "/ipfs/" + cid;

        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        boolean isWritten = false;
        while(pIte.hasNext()){
            PeerAddresses addr = pIte.next();
            if(addr.peerId.toString().equals(node1Id.toString())){
                if(isWritten){
                    continue;
                }
                byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(data, pathToPublish, expiry,
                        Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl);
                CborObject cbor = CborObject.fromByteArray(cborEntryData);
                CborObject.CborMap map = (CborObject.CborMap) cbor;
                String str_cid = cid.toString();
                //ファイル書き込み
                Kad.writeMerkleDAG(str_cid, map);
                isWritten = true;
            }else{
                boolean success = dialPeer(addr, us).orTimeout(5, TimeUnit.SECONDS).join().putValue(data,"/ipfs/" + cid, expiry, sequence,
                        ttl, cid, us.getPrivKey()).join();
                retList.add(addr);
            }

        }
        return retList;

    }

    /**
     * 属性情報がある前提での呼び出しが行われるメソッド．
     * コンテンツ＋属性をPUTする．
     * @param data
     * @param cid
     * @param attrList
     * @param us
     * @return
     */
    public List<PeerAddresses> putContentWithAttrAndTag(boolean isAdd, byte[] data, Multihash cid,
                                                  List<HashMap<String, String>> attrList, List<HashMap<String, String> > tagList, Host us, String ex){

        byte[] allByte;
        LinkedList<byte[]> chunkList = new LinkedList<byte[]>();
        //Chunkごとにわける．
        chunkList = Kad.genChunkList(data);
        LinkedList<Cid> cidList = Kad.genCidList(chunkList);

        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, Kad.getIns().getPutRedundancy(), us);
        PeerAddresses closestPeerID = closestPeers.get(0);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();
        //MerkleDAGのputを同期処理として行う．

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        String pathToPublish = "/ipfs/" + cid;

        //属性情報から，属性情報のリストを生成する．
        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        if(isAdd){
            //まずは自身にMerkleDAGを書き込む．
            //属性付きのバイナリデータを生成する．
            byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForAttrPut( attrList, tagList, cidList, pathToPublish, expiry,
                    Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl,ex);
            CborObject cbor = CborObject.fromByteArray(cborEntryData);
            CborObject.CborMap dagMap = (CborObject.CborMap) cbor;
            String str_cid = cid.toString();

            //DAGのファイル書き込み
            Kad.writeMerkleDAGOnly(str_cid, dagMap);
        }

        //MerkleDAG自体のputのためのループ
        while(pIte.hasNext()){
            PeerAddresses addr = pIte.next();
            //put先が自分自身であれば，MerkleDAGを書き込む．
            //この時点でcidリストを入れるべきである．
            if(addr.peerId.toString().equals(node1Id.toString())){
                //  if(!isAdd){
                //まずは自身にMerkleDAGを書き込む．
                //属性付きのバイナリデータを生成する．
                byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForAttrPut( attrList, tagList, cidList, pathToPublish, expiry,
                        Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl,ex);
                CborObject cbor = CborObject.fromByteArray(cborEntryData);
                CborObject.CborMap dagMap = (CborObject.CborMap) cbor;
                String str_cid = cid.toString();

                //DAGのファイル書き込み
                Kad.writeMerkleDAGOnly(str_cid, dagMap);
                // }
                //  continue;
            }else{
                //MerkleDAGのPUT処理
                boolean success = dialPeer(addr, us).orTimeout(5, TimeUnit.SECONDS).join().
                        putValueWithAttr(attrList,tagList, cidList,"/ipfs/" + cid, expiry, sequence,
                                ttl, cid, us.getPrivKey(),ex).join();

                retList.add(addr);
            }


        }

        //各指定された属性情報に関してfindNodeをして担当ノードを探す．そして，コンテンツをputされたピアIDを，
        // 担当ノードに対してDBに登録してもらう．(コンテンツput先がA，time^08の担当ノードがBだとすると，
        //Bに対して，「Aにはtime^08があるよ」という情報をBへ登録する．
        //addrがあるので，これを使って，当該クライアントで登録してもらう．
        //例えばtime_0825であるばあい，time^08* を持つ担当者を探し，その担当者へDB登録依頼をしてもらう．
        //値のフィルタを，とりあえず2とする．
        Iterator<HashMap<String, String>> mIte = attrList.iterator();
        while(mIte.hasNext()){
            HashMap<String, String> map = mIte.next();
            //key/valueを認識させる．
            Iterator<String> kIte = map.keySet().iterator();
            while(kIte.hasNext()){
                String key = kIte.next();
                String value = map.get(key);

                //String mask = value.substring(0, 2);
                //time^0824
                //String mask = value;

                //String maskAttr = Kad.java.genAttrMask(key, mask);
                String maskAttr = Kad.genAttrMask(key, value);
                String maskInfo = Kad.genAttrInfo(key, value);
                Cid attrCid = Kad.genCid(maskAttr);
                //attrCidをもって，担当ノードを探す．
                List<PeerAddresses> mgrNodes = this.findInCharge(attrCid, us, maskAttr);
                if(mgrNodes.isEmpty()){
                    //もし担当者が見つからなければ，どうするか・・・・．
                    //担当者を決める？？？
                    List<PeerAddresses> newList = this.findMgrFromEndpoint(cid,us,maskAttr);
                    if(!newList.isEmpty()){
                        mgrNodes.add(newList.get(0));
                    }
                }
                //ここを1つのみにしてよいかどうかという問題がある．
                PeerAddresses mgrTarget = mgrNodes.get(0);
                //担当ノードへ，DB登録をしてもらう．
                if(mgrTarget.peerId.toString().equals(node1Id.toString())){
                    //担当ノード = 自分であれば，DBに登録して終わり．
                    //cid(time^08), time^08, prividerのaddr
                    //cid(time^08), time^08, CIDを登録する．
                    Kad.getIns().getStore().putAttrLink(attrCid.toString(), maskInfo, cid.toString());
                }else{
                    //addrは，属性のPUT先
                    dialPeer(mgrTarget, us).orTimeout(5, TimeUnit.SECONDS).join().
                            putAttrInfo(maskInfo.getBytes(),true,"/ipfs/" + attrCid, expiry, sequence,
                                    ttl, attrCid, us.getPrivKey(), mgrTarget, cid.toString()).join();
                }

            }
        }

        //次は，TAGのput
        Iterator<HashMap<String, String>> tIte = tagList.iterator();
        while(tIte.hasNext()){
            HashMap<String, String> map = tIte.next();
            //key/valueを認識させる．
            Iterator<String> kIte = map.keySet().iterator();
            while(kIte.hasNext()){
                String key = kIte.next();
                String value = map.get(key);

                //String maskAttr = Kad.java.genAttrMask(key, mask);
                String tagInfo = Kad.genTagInfo(key, value);
                Cid tagCid = Kad.genCid(tagInfo);
                //完全一致用
                this.registerTagInfo(tagCid, us, node1Id, tagInfo, key, value, cid, null);
                //次に，部分一致検索用の処理をする．
                String mask = Kad.genTagMask(key, value);
                Cid tagMaskCid = Kad.genCid(mask);
                this.registerTagInfo(tagMaskCid, us, node1Id, mask, key, value, cid, null);

            }
        }


        //各チャンクの非同期PUTを行う．
        if(chunkList.size() >= 1){
            Iterator<byte[]> cIte = chunkList.iterator();
            while(cIte.hasNext()){
                byte[] chunk = cIte.next();
                Cid ccid = Kad.genCid(chunk);
                Kad.chunkExec.submit(new ChunkPutThread(isAdd, this, chunk,  ccid, us,ex));

            }

        }

        return retList;

    }

    public void registerTagInfo(Cid tagCid, Host us, Multihash node1Id, String tagInfo, String key,
                                String value, Multihash cid,PeerAddresses addr){
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        //attrCidをもって，担当ノードを探す．
        //List<PeerAddresses> mgrNodes = this.findInCharge(attrCid, us, maskAttr);
        //完全一致検索用の処理
        //CID(Tag名^Tag値)に近いノードを探す．
        List<PeerAddresses> mgrNodes = this.findClosestPeers(tagCid, Kad.getIns().getPutRedundancy(), us);

        if(mgrNodes.isEmpty()){

        }
        Iterator<PeerAddresses> mgrIte = mgrNodes.iterator();
        while(mgrIte.hasNext()) {

            PeerAddresses mgrTarget = mgrIte.next();
            //担当ノードへ，DB登録をしてもらう．
            if (mgrTarget.peerId.toString().equals(node1Id.toString())) {
                //担当ノード = 自分であれば，DBに登録して終わり．
                //cid(time^08), time^08, prividerのaddr
                //cid(time^08), time^08, CIDを登録する．
                //Kad.java.getIns().getStore().putAttrLink(attrCid.toString(), maskInfo, cid.toString());
                Kad.getIns().getStore().putTagInfo(tagCid.toString(), tagInfo, key, value, cid.toString() );
            } else {
                //Tag情報のPUT
                //addrは，属性のPUT先
                dialPeer(mgrTarget, us).orTimeout(5, TimeUnit.SECONDS).join().
                        putTagInfo(key, value, tagInfo, tagCid.toString(),  "/ipfs/" + tagCid, expiry, sequence,
                                ttl, tagCid, us.getPrivKey(), addr, cid.toString()).join();


            }
        }
    }


    /**
     * 担当ノード決めのメソッド
     * @param cid
     * @param us
     * @param mask
     * @return
     */
    public List<PeerAddresses> findPutTarget(Multihash cid, Host us, String mask){
        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, 1, us);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();
        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        while(pIte.hasNext()){
            PeerAddresses addrs = pIte.next();
            if(addrs.peerId.toString().equals(node1Id.toString())){
                retList.add(addrs);
                break;
                //continue;
            }else{
                Dht.Record res = this.dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().
                        askInCharge(mask).join();

                ByteString bs = res.getValue();
                CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
                retList.add(addrs);
                break;
            }
            //retList.add(addrs);
        }
        return retList;
    }


    /**
     * 担当ノードを探す．
     * @param cid
     * @param us
     * @param mask
     * @return
     */
    public List<PeerAddresses> findInCharge(Multihash cid, Host us, String mask){
        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, Kad.beta, us);
        //List<PeerAddresses> closestPeers = this.engine.getKClosestPeers(cid.toBytes());
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();
        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        while(pIte.hasNext()){
            PeerAddresses addrs = pIte.next();
            if(addrs.peerId.toString().equals(node1Id.toString())){
                if(Kad.getIns().getStore().isInCharge(mask)){
                    retList.add(addrs);
                    break;
                }
                //continue;
            }else{
                Dht.Record res = this.dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().
                        askInCharge(mask).join();

                ByteString bs = res.getValue();
                CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
                CborObject.CborBoolean ret = (CborObject.CborBoolean) cbor;

                //もし担当ノードであれば，listへaddする．そしてbreakする．
                if(ret.value){
                    retList.add(addrs);
                    break;

                }
            }
            //retList.add(addrs);
        }
        if(retList.isEmpty()){
            //それでも担当ノードがなければ，bootstrapへ聞きに行く．
            List<PeerAddresses> nextList = findMgrFromEndpoint(cid, us, mask);
            if(!nextList.isEmpty()){
                retList.add(nextList.get(0));
            }
        }
        return retList;
    }

    public List<PeerAddresses> findMgrFromEndpoint(Multihash cid, Host us, String mask){
        //List<PeerAddresses> closestPeers = this.findClosestPeers(cid, Kad.java.beta, us);
        //List<PeerAddresses> closestPeers = this.engine.getKClosestPeers(cid.toBytes());
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();
        //Iterator<PeerAddresses> pIte = closestPeers.iterator();
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());
        PeerAddresses endpointAddr = Kad.genPeerAddresses(Kad.getIns().getEndPointPeerId());

        Dht.Record res = this.dialPeer(endpointAddr, us).
                orTimeout(5, TimeUnit.SECONDS).join().findMgr(mask).join();
        ByteString bs = res.getValue();
        CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
        CborObject.CborString ret = (CborObject.CborString) cbor;
        String mgrAddr;
        if(ret != null){
            mgrAddr = ret.value;
            PeerAddresses mgrAddresses = Kad.genPeerAddresses(mgrAddr);
            retList.add(mgrAddresses);
        }

        return retList;
    }

    public List<PeerAddresses> putAttr(byte[] data, Multihash cid, Host us, PeerAddresses pred, PeerAddresses suc){
        //1ピアのみ取得する．
        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, 1, us);
        PeerAddresses closestPeerID = closestPeers.get(0);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

/*
       boolean success = dialPeer(closestPeerID, us).orTimeout(5, TimeUnit.SECONDS).join().putValue("/ipfs/" + cid, expiry, sequence,
                ttl, node1Id, us.getPrivKey()).join();
*/

        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        while(pIte.hasNext()){
            PeerAddresses addrs = pIte.next();
            if(addrs.peerId.toString().equals(node1Id.toString())){
                //continue;
            }

            boolean success = dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().putAttr(data,"/ipfs/" + cid, expiry, sequence,
                    ttl, cid, us.getPrivKey(), pred, suc).join();
            retList.add(addrs);
        }

        //return closestPeerID;
        return retList;

    }

    /**
     *
     * @param data byte(属性名^値)
     * @param cid dataのcid値
     * @param us
     * @param addrs put先のピア
     * @param pred
     * @param suc
     * @return
     */
    public List<PeerAddresses> putAttrWithTarget(byte[] data, Multihash cid, Host us, PeerAddresses addrs, PeerAddresses pred, PeerAddresses suc){
        //1ピアのみ取得する．
        //List<PeerAddresses> closestPeers = this.findClosestPeers(cid, 1, us);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());
        String pathToPublish = "/ipfs/" + cid;
        if(addrs.peerId.toString().equals(node1Id.toString())){
            String in_pred = null;
            String in_suc = null;
            if(pred != null ){
                in_pred = pred.toString();

            }
            if(suc != null){
                in_suc = suc.toString();
            }
            //自分自身がput対象であれば，ファイル書き込み+DB反映を行う．
            byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(data,in_pred, in_suc,true, pathToPublish, expiry,
                    Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl);
////////////////////
            CborObject cbor = CborObject.fromByteArray(cborEntryData);
            CborObject.CborMap map = (CborObject.CborMap) cbor;
            String str_cid = cid.toString();
            //ファイル書き込み
            Kad.writeMerkleDAG(str_cid, map);
            //Kad.java.writeData(str_cid, map);

            //DBへの反映
            Kad.getIns().getStore().putPredSuc(str_cid, new String(data), in_pred, in_suc);


        }else{
            boolean success = dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().putAttr(data,"/ipfs/" + cid, expiry, sequence,
                    ttl, cid, us.getPrivKey(), pred, suc).join();
            retList.add(addrs);
        }


        return retList;

    }



    /**
     * Added by Kanemitsu
     * 属性値のhashをputする．
     * @param data
     * @param cid
     * @param us
     */
    public List<PeerAddresses> putAttr(byte[] data, Multihash cid, Host us){
        //1ピアのみ取得する．
        List<PeerAddresses> closestPeers = this.findClosestPeers(cid, 2, us);
        PeerAddresses closestPeerID = closestPeers.get(0);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

/*
       boolean success = dialPeer(closestPeerID, us).orTimeout(5, TimeUnit.SECONDS).join().putValue("/ipfs/" + cid, expiry, sequence,
                ttl, node1Id, us.getPrivKey()).join();
*/

        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        while(pIte.hasNext()){
            PeerAddresses addrs = pIte.next();
            if(addrs.peerId.toString().equals(node1Id.toString())){
                continue;
            }

            boolean success = dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().putAttr(data,"/ipfs/" + cid, expiry, sequence,
                    ttl, cid, us.getPrivKey()).join();
            retList.add(addrs);
        }
        //return closestPeerID;
        return retList;

    }

    public CompletableFuture<List<CborObject.CborMap>> getValueByAttrs2(HashMap<String, Cborable> mapMap,
                                                                        Multihash currentFilterCid,
                                                                        PeerAddresses peer,
                                                                        String attrName,
                                                                        String attrCurrent,
                                                                        String attrMax,
                                                                        Host us,
                                                                        boolean isCidOnly){
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        //cid = cid(time^08), str_cid
        //hashに近い指定数ピアを取得する．
        //List<PeerAddresses> closestPeers = findClosestPeers(currentFilterCid, Kad.java.beta, us);
        List<IpnsRecord> candidates = new ArrayList<>();
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
        long ttl = 1 * 3600_000_000_000L;

        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();
        //Multihash hash = Multihash.fromBase58("/ipfs"+publisher.toBase58());
        //このハッシュではない．
        CborObject.CborMap map = null;
        queriedPeers.add(peer.peerId);
        //IpnsRecordのbyote[]を取得する．
        Dht.Record res = dialPeer(peer, us).orTimeout(5, TimeUnit.SECONDS).join().
                getValueByAttrMask(mapMap, "/ipfs/" + currentFilterCid.toString(), attrName, attrCurrent, attrMax, expiry, sequence,
                        ttl, currentFilterCid, us.getPrivKey(), isCidOnly).join();
        ByteString bs = res.getValue();
        CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
        map =  (CborObject.CborMap) cbor;
        mapList.add(map);



        // Validate and sort records by sequence number
        List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        return CompletableFuture.completedFuture(mapList);
    }


    public /*CompletableFuture<CborObject.CborMap>*/HashMap<String, Cborable> getAttrInfoAgain(HashMap<String, Cborable> mapMap, PeerId src, Multihash attrCid,
                                                                 String attrName,
                                                                 String attrCurrent,
                                                                 String attrMax,
                                                                 Host us
                                                                 ){


        List<PeerAddresses> closestPeers = this.findClosestPeers(attrCid, Kad.beta, us);
        List<PeerAddresses> retList2 = new LinkedList<PeerAddresses>();
        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
        HashMap<String, Cborable> retMap = new HashMap<String, Cborable>();
        Set<Multihash> queriedPeers = new HashSet<>();

        queriedPeers.add(node1Id);

        while(pIte.hasNext()){
            PeerAddresses addrs = pIte.next();
            if(queriedPeers.contains(addrs.peerId)){
                continue;
            }
            if(addrs.peerId.toString().equals(node1Id.toString())){
                continue;
            }else{
                //Cid attrCid = Kad.java.genCid(attrMask);
                long ttl = 1 * 3600_000_000_000L;
                LocalDateTime expiry = LocalDateTime.now().plusHours(1);
                int sequence = 1;
                if(addrs.peerId.toString().indexOf(src.toString())!= -1){
                    continue;
                }
                //System.out.println("addrs:" + addrs.peerId.toString() + "/ src: " + src.toString());


                Dht.Record res  = dialPeer(addrs, us).orTimeout(15, TimeUnit.SECONDS).join().
                        getValueByAttrMask(mapMap, "/ipfs/" + attrCid.toString(), attrName, attrCurrent, attrMax, expiry, sequence,
                                ttl, attrCid, us.getPrivKey(), true).join();
                ByteString bs = res.getValue();
                CborObject cbor2 = CborObject.fromByteArray(bs.toByteArray());
                CborObject.CborMap map =  (CborObject.CborMap) cbor2;
                if(!map.keySet().isEmpty()){
                    mapList.add(map);

                }

                Iterator<CborObject.CborMap> ite = mapList.iterator();
                while(ite.hasNext()){
                    CborObject.CborMap cMap = ite.next();
                    //cMapのキー自体がcid
                    Iterator<String> cIte = cMap.keySet().iterator();
                    while(cIte.hasNext()){
                        String ccid = cIte.next();
                        //CborObject.CborMap m = (CborObject.CborMap) cMap.get(ccid);
                        retMap.put(ccid, new CborObject.CborString(ccid));
                    }
                    // String cid = ((CborObject.CborString)cMap.get("cid")).value;
                    //retMap.put(cid, cMap);
                }

                //CborObject.CborMap allMap = CborObject.CborMap.build(retMap);

                // Validate and sort records by sequence number
                //List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
                //return CompletableFuture.completedFuture(allMap);
                //queriedPeers.add(peer.peerId);
            }
            //retList.add(addrs);
            queriedPeers.add(addrs.peerId);

        }
        return retMap;


    }

    /**
     * EndpointへMgrリストを登録する．
     * @param state
     */
    public void registerMgrAtEndPoint(SortedMap<String, Cborable> state, Host us){
        PeerAddresses endpointAddr = Kad.genPeerAddresses(Kad.getIns().getEndPointPeerId());

        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        HashMap<String, Cborable> retMap = new HashMap<String, Cborable>();
        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;

        this.dialPeer(endpointAddr, us).
                orTimeout(5, TimeUnit.SECONDS).join().registerMgr(state, expiry, sequence, ttl, null,
                        us.getPrivKey()).join();
        //System.out.println();
    }
    /**
     * 指定したタグ情報を下に，それに紐づいているCIDリストを返します．
     * @param tagCid
     * @param tagName
     * @param tagValue
     * @param us
     * @return
     */
    public CompletableFuture<CborObject.CborMap> getCidByTags(Multihash tagCid,
                                                                 String tagName,
                                                                 String tagValue,
                                                                 Host us
                                                                 ){
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();
        HashMap<String, Cborable> retMap = new HashMap<String, Cborable>();
        int sequence = 1;
        //cid = cid(time^08), str_cid
        //hashに近い指定数ピアを取得する．
        String initMask = Kad.genTagInfo(tagName, tagValue);

        HashSet<String> cidSet0 = Kad.getIns().getStore().getTagInfo(tagCid.toString());
        Iterator<String> cidIte0= cidSet0.iterator();
        while(cidIte0.hasNext()){
            String cid = cidIte0.next();
            retMap.put(cid, new CborObject.CborString(cid));
        }

        //Tag名^Tag値のCIDに近いβ個のピアを取得する．
        List<PeerAddresses> closestPeers = findClosestPeers(tagCid, Kad.beta, us);

        //PeerAddresses mgr = mgrNodes.get(0);
        //List<PeerAddresses> closestPeers = findClosestPeers(currentFilterCid, Kad.java.beta, us);
        List<IpnsRecord> candidates = new ArrayList<>();
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
        long ttl = 1 * 3600_000_000_000L;

        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();

        for (PeerAddresses peer : closestPeers) {
            if (queriedPeers.contains(peer.peerId))
                continue;
            if(us.getPeerId().toString().equals(peer.peerId.toString())){
                //もし自分自身がTag情報を持っていれば，DBからCIDリストを取得して
                //格納する．
                HashSet<String> cidSet = Kad.getIns().getStore().getTagInfo(tagCid.toString());
                Iterator<String> cidIte = cidSet.iterator();
                while(cidIte.hasNext()){
                    String cid = cidIte.next();
                    retMap.put(cid, new CborObject.CborString(cid));
                }


            }else{
                //IpnsRecordのbyote[]を取得する．
                Dht.Record res = dialPeer(peer, us).orTimeout(5, TimeUnit.SECONDS).join().
                        getCidByTags("/ipfs/" + tagCid.toString(), tagName, tagValue, expiry, sequence,
                                ttl, tagCid,us.getPrivKey()).join();
                ByteString bs = res.getValue();
                CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
                CborObject.CborMap map =  (CborObject.CborMap) cbor;
                Iterator<String> cIte = map.keySet().iterator();
                while(cIte.hasNext()){
                    String key = cIte.next();
                    retMap.put(key, new CborObject.CborString(key));
                }


            }
            queriedPeers.add(peer.peerId);
        }

        CborObject.CborMap allMap = CborObject.CborMap.build(retMap);

        // Validate and sort records by sequence number
        //List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        return CompletableFuture.completedFuture(allMap);

    }


    /**
     *
     * @param currentFilterCid 現在のHash(属性名^値)
     * @param attrName  属性名
     * @param attrCurrent 最小値
     * @param attrMax 最大値
     * @param us
     * @param isCidOnly
     * @return
     */
    public CompletableFuture<CborObject.CborMap> getValueByAttrs(Multihash currentFilterCid,
                                                                 String attrName,
                                                                 String attrCurrent,
                                                                 String attrMax,
                                                                 Host us,
                                                                 boolean isCidOnly){
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);
        List<PeerAddresses> retList = new LinkedList<PeerAddresses>();

        int sequence = 1;
        //cid = cid(time^08), str_cid
        //hashに近い指定数ピアを取得する．
        String initMask = Kad.genAttrMask(attrName, attrCurrent);
        List<PeerAddresses> mgrNodes = this.findInCharge(currentFilterCid, us,initMask );
        //PeerAddresses mgr = mgrNodes.get(0);

        //現状は，一つのノードのみを取得する
        PeerAddresses mgrTarget = mgrNodes.get(0);
        //List<PeerAddresses> closestPeers = findClosestPeers(currentFilterCid, Kad.java.beta, us);
        List<IpnsRecord> candidates = new ArrayList<>();
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
        long ttl = 1 * 3600_000_000_000L;

        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();
        //Multihash hash = Multihash.fromBase58("/ipfs"+publisher.toBase58());
        //このハッシュではない．
        CborObject.CborMap map = null;
        //IpnsRecordのbyote[]を取得する．
        Dht.Record res = dialPeer(mgrTarget, us).orTimeout(5, TimeUnit.SECONDS).join().
                getValueByAttr("/ipfs/" + currentFilterCid.toString(), attrName, attrCurrent, attrMax, expiry, sequence,
                        ttl, currentFilterCid, us.getPrivKey(), isCidOnly).join();
        ByteString bs = res.getValue();
        CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
        map =  (CborObject.CborMap) cbor;
        if(!map.keySet().isEmpty()){
            mapList.add(map);

        }

        //属性値のハッシュに近いピアたちのループ
        /*for (PeerAddresses peer : closestPeers) {
            if (queriedPeers.contains(peer.peerId))
                continue;
            if(us.getPeerId().toString().equals(peer.peerId.toString())){
                HashMap<String, Cborable> allMap = this.engine.processAttrTransfer(
                        attrName, attrCurrent, attrMax, isCidOnly);
                if(allMap.isEmpty()){

                }else{
                    CborObject.CborMap cMap = CborObject.CborMap.build(allMap);
                    mapList.add(cMap);
                }

            }else{
                //IpnsRecordのbyote[]を取得する．
                Dht.Record res = dialPeer(peer, us).orTimeout(5, TimeUnit.SECONDS).join().
                        getValueByAttr("/ipfs/" + currentFilterCid.toString(), attrName, attrCurrent, attrMax, expiry, sequence,
                                ttl, currentFilterCid, us.getPrivKey(), isCidOnly).join();
                ByteString bs = res.getValue();
                CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
                map =  (CborObject.CborMap) cbor;
                if(!map.keySet().isEmpty()){
                    mapList.add(map);

                }

            }
            //queriedPeers.add(peer.peerId);

        }*/

        Iterator<CborObject.CborMap> ite = mapList.iterator();
        System.out.println("NUM:" + mapList.size());
        HashMap<String, Cborable> retMap = new HashMap<String, Cborable>();
        while(ite.hasNext()){
            CborObject.CborMap cMap = ite.next();
            //cMapのキー自体がcid
            Iterator<String> cIte = cMap.keySet().iterator();
            while(cIte.hasNext()){
                String ccid = cIte.next();
                //CborObject.CborMap m = (CborObject.CborMap) cMap.get(ccid);
                retMap.put(ccid, new CborObject.CborString(ccid));
            }
           // String cid = ((CborObject.CborString)cMap.get("cid")).value;
            //retMap.put(cid, cMap);
        }

        CborObject.CborMap allMap = CborObject.CborMap.build(retMap);

        // Validate and sort records by sequence number
        //List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        return CompletableFuture.completedFuture(allMap);
    }


    public CompletableFuture<List<CborObject.CborMap>> getValueWithMerkleDAG(Multihash cid, String str_cid, Host us) {
        //hashに近い指定数ピアを取得する．
        List<PeerAddresses> closestPeers = findClosestPeers(cid, Kad.beta, us);
        List<IpnsRecord> candidates = new ArrayList<>();
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();

        CborObject.CborMap ownMap = Kad.readMerkleDAGOnly(cid.toString());
        //自身で持っていればそこで終了．
        if(ownMap != null){
            mapList.add(ownMap);
            return CompletableFuture.completedFuture(mapList);
        }
        //以降は，自身がMerkleDAGを持っていないときの処理
        Set<PeerAddresses> queryCandidates = new HashSet<>();
        Set<Multihash> queriedPeers = new HashSet<>();
        //Multihash hash = Multihash.fromBase58("/ipfs"+publisher.toBase58());
        //このハッシュではない．
        Multihash hash = Multihash.deserialize(us.getPeerId().getBytes());
        CborObject.CborMap map = null;
        for (PeerAddresses peer : closestPeers) {
            if (queriedPeers.contains(peer.peerId))
                continue;
            if(us.getPeerId().toString().equals(peer.peerId.toString())){
                continue;
            }

            queriedPeers.add(peer.peerId);
            //IpnsRecordのbyote[]を取得する．
            Dht.Record res = dialPeer(peer, us).orTimeout(5, TimeUnit.SECONDS).join().getMerkleDAG(str_cid).join();

            ByteString bs = res.getValue();
            CborObject cbor = null;

            cbor = CborObject.fromByteArray(bs.toByteArray());
            map =  (CborObject.CborMap) cbor;
            mapList.add(map);



        }

        // Validate and sort records by sequence number
        List<IpnsRecord> records = candidates.stream().sorted().collect(Collectors.toList());
        return CompletableFuture.completedFuture(mapList);
       // return CompletableFuture.completedFuture(records.get(records.size() - 1).value);
    }

    protected static Object deepCopyObject(byte[] obj) {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        Object ret = null;

        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()));
            ret = ois.readObject();
        } catch (Exception e) {
        } finally {
            try {
                if (oos != null) {
                    oos.close();
                }
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException e) {
            }
        }

        return ret;
    }

}
