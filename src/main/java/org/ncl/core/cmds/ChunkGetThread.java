package org.ncl.kadrtt.core.cmds;

import com.google.protobuf.ByteString;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import org.ncl.kadrtt.core.Kad;
import org.peergos.PeerAddresses;
import org.peergos.cbor.CborObject;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.dht.pb.Dht;
import org.peergos.protocol.ipns.IPNS;
import org.peergos.protocol.ipns.IpnsRecord;
import org.peergos.protocol.ipns.pb.Ipns;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ChunkGetThread implements Runnable{


    private Kademlia kad;

    /**
     * 見つける方のcid
     */
    private String cid;

    private Host us;

    public ChunkGetThread(Kademlia kad, String cid, Host us) {
        this.kad = kad;
        this.cid = cid;
        this.us = us;
    }

    @Override
    public void run() {
        //まずは，指定cidを持っているかどうかを見る．
        if(Kad.isChunkExists(this.cid)){
            return;
        }else{
            Multihash multihash_cid = Multihash.fromBase58(this.cid);

            List<PeerAddresses> closestPeers = this.kad.findClosestPeers(multihash_cid, Kad.getIns().getPutRedundancy(), us);
            List<IpnsRecord> candidates = new ArrayList<>();
            List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();


            //以降は，自身がMerkleDAGを持っていないときの処理
            Set<PeerAddresses> queryCandidates = new HashSet<>();
            Set<Multihash> queriedPeers = new HashSet<>();
            //Multihash hash = Multihash.fromBase58("/ipfs"+publisher.toBase58());
            //このハッシュではない．
            CborObject.CborMap map = null;
            for (PeerAddresses peer : closestPeers) {
                if (queriedPeers.contains(peer.peerId))
                    continue;
                if(us.getPeerId().toString().equals(peer.peerId.toString())){
                    continue;
                }

                queriedPeers.add(peer.peerId);
                //IpnsRecordのbyote[]を取得する．
                Dht.Record res = this.kad.dialPeer(peer, us).orTimeout(5, TimeUnit.SECONDS).join().getChunk(this.cid).join();

                ByteString bs = res.getValue();
                CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
                LocalDateTime expiry = LocalDateTime.now().plusHours(1);

                int sequence = 1;
                long ttl = 1 * 3600_000_000_000L;
                Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());
                String pathToPublish = "/ipfs/" + cid;

                byte[] cborEntryData = IPNS.createCborDataForIpnsEntrySingle(bs.toByteArray(), pathToPublish, expiry,
                        Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl);
                CborObject cbor2 = CborObject.fromByteArray(cborEntryData);
                //CborObject.CborMap map = (CborObject.CborMap) cbor;
                CborObject.CborMap map2 = (CborObject.CborMap) cbor2;

                String str_cid = cid.toString();
                //System.out.println("cid:"+str_cid);
                //ファイル書き込み
                //Kad.writeMerkleDAG(str_cid, map);
                Kad.writeData(str_cid, map2);

            }

        }

    }
}
