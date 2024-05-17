package org.ncl.kadrtt.core.cmds;

import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import org.ncl.kadrtt.core.Kad;
import org.peergos.PeerAddresses;
import org.peergos.cbor.CborObject;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.ipns.IPNS;
import org.peergos.protocol.ipns.pb.Ipns;

import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ChunkPutThread implements Runnable{


    private Kademlia kad;

    private byte[] data;

    private Multihash cid;

    private Host us;

    private LinkedList<byte[]> chunkList;



    public ChunkPutThread(Kademlia kad, byte[] data, Multihash cid, Host us) {
        this.kad = kad;
        this.data = data;
        this.cid = cid;
        this.us = us;

    }

    public ChunkPutThread(Kademlia kad, LinkedList<byte[]> chunkList,  Host us) {
        this.kad = kad;
        this.chunkList = chunkList;
        this.us = us;

    }

    /**
     * chunkListにおける，2つめのchunkのputに関するスレッド．
     *
     */
    @Override
    public void run() {
        /*Iterator<byte[]> cIte = this.chunkList.iterator();
        while(cIte.hasNext()){
            byte[] data = cIte.next();
            Cid cid = Kad.genCid(data);
            //1ピアのみ取得する．
            List<PeerAddresses> closestPeers = this.kad.findClosestPeers(cid, Kad.getIns().getPutRedundancy(), us);
            LocalDateTime expiry = LocalDateTime.now().plusHours(1);

            int sequence = 1;
            long ttl = 1 * 3600_000_000_000L;
            Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

            String pathToPublish = "/ipfs/" + cid;

            Iterator<PeerAddresses> pIte = closestPeers.iterator();
            boolean isWritten = false;
            // Kad.writeData
            //Closestピアに対するループ
            while(pIte.hasNext()){
                PeerAddresses addr = pIte.next();
                //宛先が自分以外のときだけ通信処理を行う．この場合は，現在対象としているチャンクを
                //リモートノードへputする．
                if(!addr.peerId.toString().equals(node1Id.toString())){
                    boolean success = this.kad.dialPeer(addr, us).orTimeout(5, TimeUnit.SECONDS).join().
                            putRemainedChunk(data, "/ipfs/" + cid, expiry, sequence,
                            ttl, cid, us.getPrivKey()).join();
                }
            }

            System.out.println("***WRITE:"+cid.toString());
            //あとは自身のディレクトリに書き込み処理をする．
            byte[] cborEntryData = IPNS.createCborDataForIpnsEntrySingle(data, pathToPublish, expiry,
                    Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl);
            CborObject cbor = CborObject.fromByteArray(cborEntryData);
            CborObject.CborMap map = (CborObject.CborMap) cbor;
            String str_cid = cid.toString();
            //System.out.println("cid:"+str_cid);
            //ファイル書き込み
            //Kad.writeMerkleDAG(str_cid, map);
            Kad.writeData(str_cid, map);

        }
        */

        int cnt = 0;

        //1ピアのみ取得する．
        List<PeerAddresses> closestPeers = this.kad.findClosestPeers(cid, Kad.getIns().getPutRedundancy(), us);
        LocalDateTime expiry = LocalDateTime.now().plusHours(1);

        int sequence = 1;
        long ttl = 1 * 3600_000_000_000L;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        String pathToPublish = "/ipfs/" + cid;
        Iterator<PeerAddresses> pIte = closestPeers.iterator();
        boolean isWritten = false;
        // Kad.writeData
        //Closestピアに対するループ
        while(pIte.hasNext()){
            PeerAddresses addr = pIte.next();
            if(addr.peerId.toString().equals(node1Id.toString())){
                //continue;
                // isWritten = true;
            }else{
                try{

                    this.kad.dialPeer(addr, us).orTimeout(5, TimeUnit.SECONDS).join().putRemainedChunk(data, "/ipfs/" + cid, expiry, sequence,
                            ttl, cid, us.getPrivKey()).join();
                }catch(Exception e){
                    e.printStackTrace();
                }


            }

        }


        //あとは自身のディレクトリに書き込み処理をする．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntrySingle(data, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttl);
        CborObject cbor = CborObject.fromByteArray(cborEntryData);
        CborObject.CborMap map = (CborObject.CborMap) cbor;
        String str_cid = cid.toString();
        //System.out.println("cid:"+str_cid);
        //ファイル書き込み
        //Kad.writeMerkleDAG(str_cid, map);
        Kad.writeData(str_cid, map);

    }


}
