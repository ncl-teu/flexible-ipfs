package org.peergos.protocol.dht;

import com.google.protobuf.*;
import com.offbynull.kademlia.*;
import io.ipfs.cid.*;
import io.ipfs.multiaddr.*;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.*;
import io.libp2p.core.Stream;
import io.libp2p.core.multiformats.*;
import org.ncl.kadrtt.core.AttrBean;
import org.ncl.kadrtt.core.Kad;
import org.peergos.*;
import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;
import org.peergos.protocol.ipns.pb.Ipns;

import java.time.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.*;

public class KademliaEngine {

    private final ProviderStore providersStore;
    private final RecordStore ipnsStore;
    public final Router router;
    private AddressBook addressBook;

    public KademliaEngine(Multihash ourPeerId, ProviderStore providersStore, RecordStore ipnsStore) {
        this.providersStore = providersStore;
        this.ipnsStore = ipnsStore;
        this.router = new Router(Id.create(ourPeerId.bareMultihash().toBytes(), 256), 2, 2, 2);
    }

    public void setAddressBook(AddressBook addrs) {
        this.addressBook = addrs;
    }

    public void addOutgoingConnection(PeerId peer, Multiaddr addr) {

        if (Kad.getIns().isPeerExist(peer, addr)) {
            router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
            addressBook.addAddrs(peer, 0, addr);
        }
     /*   boolean ret = Kad.getIns().checkPeer(peer, addr);
        if(ret){

        }else{
            RamAddressBook ram = (RamAddressBook) addressBook;
            ram.removePeer(peer);
        }
*/

    }

    public void addIncomingConnection(PeerId peer, Multiaddr addr) {
        if (Kad.getIns().isPeerExist(peer, addr)) {
            router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));
            addressBook.addAddrs(peer, 0, addr);

        }

    }

    public List<PeerAddresses> getKClosestPeers(byte[] key) {
        int k = 20;
        List<Node> nodes = router.find(Id.create(Hash.sha256(key), 256), k, false);
        System.out.println("Nodes: " + nodes.size());
        return nodes.stream()
                .map(n -> {
                    List<MultiAddress> addrs = addressBook.getAddrs(PeerId.fromBase58(n.getLink())).join()
                            .stream()
                            .map(m -> new MultiAddress(m.toString()))
                            .collect(Collectors.toList());
                    return new PeerAddresses(Multihash.fromBase58(n.getLink()), addrs);
                })
                .collect(Collectors.toList());
    }

    /**
     * 担当ノードが要求受信した後，Leaf(自分or他ノード)へリクエスト＋受信するメソッド
     * @param msg
     * @param attrName
     * @param attrCurrent
     * @param attrMax
     * @return <CID, CborMapのデータ>
     **/
    public HashMap<String, Cborable> processAttrTransfer(Dht.Message msg,
                                                         Stream stream,
                                                         String attrName,
                                                         String attrCurrent,
                                                         String attrMax,
                                                         boolean isCidOnly){
        String attrInfo = Kad.genAttrMask(attrName, attrCurrent);
        String attrInfoMax = Kad.genAttrMask(attrName, attrMax);
        //DBから，time^08をキーにして，DBから文字列 (time^08)とaddr, cid を取得する．
        //そして，dialPeerでaddrとcidを渡して情報を検索する．
        //<PeerID, <CID, bean(attrmask, addr)>
        //指定の属性情報で，Leafノードを検索する．
        HashMap<String, HashMap<String, AttrBean>> attrMap = Kad.getIns().getStore().getAttrLink(attrInfo);
        CborObject.CborMap map = null;

        //あとは，peerIdごとに問い合わせるためのループ
        //HashMap<CID, Cbor>

        int sequence = 1;
        //cid = cid(time^08), str_cid
        //hashに近い指定数ピアを取得する．
        List<IpnsRecord> candidates = new ArrayList<>();
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
        HashMap<String, Cborable> mapMap = new HashMap<String, Cborable>();
        long ttl = 1 * 3600_000_000_000L;

        LocalDateTime expiry = LocalDateTime.now().plusHours(1);

        Iterator<String> pIte = attrMap.keySet().iterator();
        HashSet<String> getSet = new HashSet<String>();

        HashMap<String, byte[]> retMap = new HashMap<String, byte[]>();
        //ピアごとのループ
        //各Leafノードに対するループ
        while(pIte.hasNext()){
            String pID = pIte.next();
            //<cid, bean>のMap
            HashMap<String, AttrBean> bMap = attrMap.get(pID);
            AttrBean sampleBean = bMap.values().iterator().next();
            String strAddr = sampleBean.getAddr();

            PeerAddresses addrs = Kad.genPeerAddresses(strAddr);
            Host us = Kad.getIns().node;
            String sampleCid = sampleBean.getCid();

            Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());
            //ノード(自・他）から，複数のcidに対応したmapを取得する．
            //recursiveで行うほうが負荷分散となる．
            //担当Aでのmap集合を取得→sucを見て，担当Bへ問い合わせる．担当Bは，
            //map集合取得→sucを見る．もしcurrent == maxであれば，map取得で修了．
            //そして，writeAndFlushする．
            //同期処理となる．
            //
            //Leafが自ノードであれば，ローカルから取得する．
            if(addrs.peerId.toString().equals(node1Id.toString())){
                Iterator<String> cIte = bMap.keySet().iterator();
                while(cIte.hasNext()){
                    String icid = cIte.next();
                    if(getSet.contains(icid)){
                        continue;
                    }
                    //byte[] content = Kad.getDataFromMerkleDAG(icid);
                    // CborObject cbor2 = CborObject.fromByteArray(content);
                    // map =  (CborObject.CborMap) cbor2;
                    CborObject.CborMap myMap = Kad.readMerkleDAG(icid);
                    if(myMap == null){
                        return null;
                    }

                    //もしcidonlyフラグが立っていれば，この時点で削除する．
                    if(isCidOnly){
                        myMap.put("RawData", new CborObject.CborString(null));

                    }
                    myMap.put("cid", new CborObject.CborString(icid));
                    //CborMapに，<cid,cborMap>で格納する．
                    mapMap.put(icid, myMap);
                    getSet.add(icid);

                    //mapList.add(map);


                }
            }else{
                //Leafが他ノードの場合
                Cid idCid = Kad.genCid(sampleCid);
                ///他ノードにお願いする場合の処理
                //同一ピアに複数のcidが入っているかもしれないので，それをまとめて問い合わせる．
                //とりあえず最初のもののcidを設定しておく．
                //戻り値はリストを含んだRecord．(CborMapのリスト)
                CborObject.CborList attrList = null;

                Dht.Record res = Kad.getIns().getKadDHT().dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().
                        getValueByAttrMask("/ipfs/" + sampleCid,bMap, attrInfo, expiry, sequence,
                                ttl, idCid, us.getPrivKey(), isCidOnly).join();
                ByteString bs = res.getValue();
                CborObject cbor_res = CborObject.fromByteArray(bs.toByteArray());
                //CborMapのリストを取得する．
                attrList = (CborObject.CborList)cbor_res;


                //Merkle DAGのリストが帰ってくる．

                Iterator<CborObject.CborMap> ite = (Iterator<CborObject.CborMap>) attrList.value.iterator();
                while(ite.hasNext()){
                    CborObject.CborMap m = ite.next();
                    String tmpCid = ((CborObject.CborString)m.get("cid")).value;
                    mapMap.put(tmpCid, m);

                }

            }

        }
        if(attrCurrent.equals(attrMax)){
            //この時点でmax値であれば，ここで打ち止め．
            //集約したデータを返す．

        }else{
            //Sucとなる担当ノードのエンドポイントを取得する．
            int nextVal = Integer.valueOf(attrCurrent).intValue() + 1;
            //String nextAttr = Kad.genAttrMask(attrName, nextVal);
            String nextAttr = Kad.genNormalizedValue(nextVal);
            HashMap<String ,String> sucMap = Kad.getIns().getStore().getSuc(attrInfo);
            String sucPeerAddress = sucMap.get("suc");
            if(sucPeerAddress == null){
                //レコードが無い場合は，無視
            }else{
                String succid = sucMap.get("cid");
                PeerAddresses sucAddr = Kad.genPeerAddresses(sucPeerAddress);
                //DBから，SUCの担当ノードを特定する．
                //SUCの担当ノードが，また自分かどうかのチェックが必要．

                if(Kad.isOwnHost(sucAddr)){
                    //sucAddr, 属性名，属性情報, 最大値が必要
                    //GET_VALUE_WITH_ATTRSのメッセージをもって呼び出す．
                    HashMap<String, Cborable> recursiveMap = this.processAttrTransfer(msg, stream, attrName, nextAttr, attrMax, isCidOnly);
                    mapMap.putAll(recursiveMap);

                }else{
                    try{
                        //SUCが他ノードであれば，リクエストを転送する．
                        CompletableFuture<List<CborObject.CborMap>> retList2 = Kad.getIns().getKadDHT().
                                getValueByAttrs2(Kad.genCid(succid), sucAddr, attrName, nextAttr, attrMax, Kad.getIns().node, isCidOnly);
                        List<CborObject.CborMap> mList = retList2.get();
                        Iterator<CborObject.CborMap> mIte = mList.iterator();
                        while(mIte.hasNext()){
                            CborObject.CborMap m = mIte.next();
                            String ccid = ((CborObject.CborString)m.get("cid")).value;
                            mapMap.put(ccid, m);
                        }
                    }catch(Exception e){
                        e.printStackTrace();
                    }

                }

            }


        }

        return mapMap;


}



    public void receiveRequest(Dht.Message msg, PeerId source, Stream stream) {
        System.out.println("Received: " + msg.getType());
        switch (msg.getType()) {
            case PUT_VALUE: {
                //ipnsEntryのこと．
                //cborの取り出し方．
                //CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);
                if (mapping != null) {
                    try{

                        //ipnsStore.put(mapping.get().publisher, mapping.get().value);
                        //cborに格納されている各種値を取得する．
                        Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                        ByteString b_str = entry.getValue();
                        String path_cid = new String(b_str.toByteArray());
                        String cid = path_cid.replace("/ipfs/", "");
                        CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                        //if (! (cbor instanceof CborObject.CborMap))
                        CborObject.CborMap map = (CborObject.CborMap) cbor;

                        //MerkleDAGとしてファイルに書き出す．
                        //ファイル名は，cid

                        //あとは，これが属性PUTなのかどうか
                        //担当属性PUT
                        if(map.containsKey("isattrput")){
                            //ファイルとして属性を保存する．
                            Kad.writeMerkleDAG(cid, map);
                            String val = null;

                                CborObject.CborByteArray valarray = (CborObject.CborByteArray)map.get("RawData");
                                if(valarray != null)
                                 val = new String(valarray.value);


                            String predBytes = null;
                            String sucBytes = null;
                            if(map.containsKey("pred")){
                                CborObject.CborString pred = (CborObject.CborString) map.get("pred");
                               predBytes = pred.value;

                            }
                            if(map.containsKey("suc")){
                                CborObject.CborString suc = (CborObject.CborString) map.get("suc");
                                sucBytes = suc.value;
                            }
                            //もし属性putモードであれば，DBを見る．
                            Kad.getIns().getStore().putPredSuc(cid, val, predBytes, sucBytes);

                        }else if(map.containsKey("isproviderput")){
                            //プロバイダのアドレス情報PUTの場合，DBへ登録する．
                            //属性情報を取得する（例: time^08)
                            CborObject.CborByteArray valarray = (CborObject.CborByteArray)map.get("RawData");
                            String val = new String(valarray.value);

                            CborObject.CborString addr = (CborObject.CborString) map.get("addr");

                            //Multihash, MultiAddressのリストの形式
                            //12D3KooWA5pL6SFauCsKqJK7J8dzXSUGqNbSQLNQ7PkaDihvv1ZG: [/ip4/60.112.207.90/tcp/52456, /ip4/60.112.207.90/tcp/52692]
                            //String bAddr = new String(addr.value);
                            //PeerAddresses pAddr = Kad.genPeerAddresses(bAddr);
                            //val (time^08), pAddr.toString()を登録する．
                            //cid(time^08), time^08, prividerのaddr
                            Kad.getIns().getStore().putAttrLink(cid, val, addr.value);


                        }else{
                            //通常のコンテンツPUTの場合，ファイル保存する．
                            if(map.keySet().size() == 1){
                                //RawDataのみの場合，dataのみ書き込む．
                                Kad.writeData(cid, map);
                            }else{
                                Kad.writeMerkleDAG(cid, map);

                            }
                        }


                    }catch(Exception e){
                        e.printStackTrace();
                    }

                    stream.writeAndFlush(msg);

                }
                break;
            }


            case PUT_MERKLEDAG:
            {

                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);
                if (mapping != null) {

                     try {
                         //cborに格納されている各種値を取得する．
                         Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                         ByteString b_str = entry.getValue();
                         String path_cid = new String(b_str.toByteArray());
                         String cid = path_cid.replace("/ipfs/", "");
                         CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                         //if (! (cbor instanceof CborObject.CborMap))
                         CborObject.CborMap map = (CborObject.CborMap) cbor;

                         //ファイル書き込み
                         Kad.writeMerkleDAGOnly(cid, map);

                     }catch(Exception e){
                         e.printStackTrace();
                     }
                    stream.writeAndFlush(msg);

                }
                break;

            }
            //担当ノードからのコンテンツ取得依頼が来たとき
            case GET_VALUE_AT_LEAF:{
                //まずはリクエストから，要求内容を取り出す．
                //基本，DBにあるはず．
                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);
                if (mapping.isPresent()) {
                    try{
                        Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                        CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                        CborObject.CborMap map = (CborObject.CborMap) cbor;
                        //mapが，CborMapのこと．
                        //<cid, CborMap>という形式
                        Iterator<String> cIte = map.keySet().iterator();
                        LinkedList<Cborable> retList = new LinkedList<Cborable>();
                        CborObject.CborBoolean isCborOnlyC = (CborObject.CborBoolean) map.get("cidonly");
                        boolean isCidOnly = isCborOnlyC.value;

                        //cidに対するループ
                        while(cIte.hasNext()){
                            String cid = cIte.next();
                            CborObject.CborMap m = (CborObject.CborMap)map.get(cid);

                            //cidについて，ディレクトリ＋DBに双方存在すればOK．
                            //あれば，ディレクトリを参照してcbor化する．
                            //まずはピアID
                            CborObject.CborString c_peerid = (CborObject.CborString) map.get("peerid");
                            String peerid = c_peerid.value;

                            CborObject.CborString c_cid = (CborObject.CborString) map.get("cid");
                            //属性情報（例: time^08)
                            CborObject.CborString c_attrmask = (CborObject.CborString) map.get("attrmask");
                            String attrmask = c_attrmask.value;
                            //この，問い合されたノードのピア接続先情報
                            CborObject.CborString c_addr = (CborObject.CborString) map.get("addr");
                            String addr = c_addr.value;

                            //まずはファイル存在チェック
                            if(Kad.isFileExists(cid)){
                                //次に，DBを見る．本来は見るべきだが，もうあるという前提で，返送する．
                                //他の属性についてはすでにcByteに含まれているはず．
                                //byte[] cByte = Kad.getDataFromMerkleDAG(cid);
                                //あとは，Mapに設定するのみ．
                                CborObject.CborMap retMap = Kad.readMerkleDAG(cid);
                                if(retMap == null){

                                }else{
                                    retMap.put("cid", new CborObject.CborString(cid));
                                    if(isCidOnly){
                                        //retMapから，RawDataを削除
                                        retMap.put("RawData", new CborObject.CborString(null));
                                    }

                                    retList.add(retMap);
                                }


                            }else{
                                continue;
                            }

                        }
                        CborObject.CborList retList2 = new CborObject.CborList(retList);
                        Dht.Message.Builder builder = msg.toBuilder();

                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom(retList2.toByteArray())));
                        stream.writeAndFlush(builder.build());

                    }catch(Exception e){
                        e.printStackTrace();
                    }

                }
                break;
            }
            //swarm keyの要求
            case QUERY_SWAM_KEY:{
                //swarmkeyを取得
               /*a String str_swarm = Kad.getIns().getSwarmKey();

                Dht.Message.Builder builder = msg.toBuilder();

                builder = builder.setRecord(Dht.Record.newBuilder()
                        .setKey(msg.getKey())
                        //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                        .setValue(ByteString.copyFrom(str_swarm.getBytes())));
                stream.writeAndFlush(builder.build());

                */
                break;
            }

            //担当ノードに対して指定の属性情報で問い合わせが来たとき
                //<CID, Cborble/CborMap>を返す．

            case GET_VALUE_WITH_ATTRS: {
                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);

                if (mapping.isPresent()) {
                    try {
                        LinkedList<byte[]> retList = new LinkedList<byte[]>();

                        ipnsStore.put(mapping.get().publisher, mapping.get().value);
                        //cborに格納されている各種値を取得する．
                        Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                        ByteString b_str = entry.getValue();

                        String path_cid = new String(b_str.toByteArray());
                        String cid = path_cid.replace("/ipfs/", "");
                        CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());

                        //if (! (cbor instanceof CborObject.CborMap))
                        CborObject.CborMap map = (CborObject.CborMap) cbor;

                        CborObject.CborString cbor_attrName = (CborObject.CborString) map.get("attrName");
                        String attrName = cbor_attrName.value;
                        CborObject.CborString cbor_attrCurrent = (CborObject.CborString) map.get("attrCurrent");
                        String attrCurrent = cbor_attrCurrent.value;
                        CborObject.CborString cbor_attrMax = (CborObject.CborString) map.get("attrMax");
                        String attrMax = cbor_attrMax.value;

                        boolean isCidOnly = false;

                        CborObject.CborBoolean isCidOnlyC = (CborObject.CborBoolean) map.get("cidonly");
                        if(isCidOnlyC.value){
                            isCidOnly = true;
                        }

                        //復号化した後の処理は関数化可能．
                        HashMap<String, Cborable> allMap = this.processAttrTransfer(msg, stream, attrName, attrCurrent, attrMax, isCidOnly);

                        byte[] retData = CborObject.CborMap.build(allMap).serialize();
                        Dht.Message.Builder builder = msg.toBuilder();
                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom(retData)));
                        stream.writeAndFlush(builder.build());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    break;
                }
            }
            //チャンク取得要求
            case GET_CHUNK: {
                String cid = msg.getKey().toStringUtf8();
                //Optional<IpnsRecord> ipnsRecord = ipnsStore.get(cid);
                Dht.Message.Builder builder = msg.toBuilder();
                CborObject.CborByteArray obj = Kad.readChunk(cid);

                if(obj != null){
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                            .setValue(ByteString.copyFrom(obj.toByteArray())));
                }else{
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                            .setValue(ByteString.copyFrom("Not Found".getBytes())));
                }
                stream.writeAndFlush(builder.build());

                break;
            }
            case GET_VALUE: {

                //ここで例外発生
                //Cid key = IPNS.getCidFromKey(msg.getKey());
                //Cid key = Cid.cast(msg.getKey().toByteArray());
                //String key = msg.getKey().toString();
                //msg.getRecord().getKey();
                //  Cid.decode(msg.getKey().toByteArray());
                //Cid key = IPNS.getCidFromKey2(msg.getKey());
                String cid = msg.getKey().toStringUtf8();

                Optional<IpnsRecord> ipnsRecord = ipnsStore.get(cid);


                Dht.Message.Builder builder = msg.toBuilder();

                //Storeにcidがあれば，Cborファイルから探す．
                //そして，RecordにCborMapをセットして返す．
              //  if (ipnsRecord.isPresent()) {
                    //ファイルを走査する．

                    CborObject.CborMap map = Kad.readMerkleDAG(cid);

                    if(map != null){
                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom(map.toByteArray())));
                    }else{
                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom("Not Found".getBytes())));
                    }

            /*    } else {
                    System.out.println("****NG!!!****");
                    builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray())
                            .stream()
                            .map(PeerAddresses::toProtobuf)
                            .collect(Collectors.toList()));

                }*/
                /*Dht.Message outgoing = Dht.Message.newBuilder()
                        //.setType(Dht.Message.MessageType.PUT_VALUE)
                        .setKey(ByteString.copyFrom(cid.getBytes()))
                        .setRecord(Dht.Record.newBuilder()
                                .setKey(ByteString.copyFrom(cid.getBytes()))
                                .setValue(ByteString.copyFrom(ipnsRecord.get().raw))
                                .build())
                        .build();
*/

                stream.writeAndFlush(builder.build());
                //stream.writeAndFlush(ipnsRecord);
                //stream.writeAndFlush(builder.build());
               // stream.writeAndFlush(outgoing);

/*
                Dht.Message response = builder.build();
                Connection con = stream.getConnection();


                LinkedList<MultiAddress> mAddrList = new LinkedList<MultiAddress>();
                //String rAstr = con.remoteAddress().toString();
                mAddrList.add(new MultiAddress(con.remoteAddress().toString()));
                Multihash remoteMultiAddr = Multihash.deserialize(source.getBytes());
                PeerAddresses remoteAddrs = new PeerAddresses(remoteMultiAddr, mAddrList);

                PeerId remotePeerId = stream.remotePeerId();
                Host us = Kad.getIns().node;
                GetResult res = Kad.getIns().getKadDHT().dialPeer(remoteAddrs, us).orTimeout(5, TimeUnit.SECONDS).join().deliverValue(response).join();
*/
                break;
            }

            case GET_MERKLEDAG:
            {

                    String cid = msg.getKey().toStringUtf8();
                    Optional<IpnsRecord> ipnsRecord = ipnsStore.get(cid);
                    Dht.Message.Builder builder = msg.toBuilder();
                    CborObject.CborMap map = Kad.readMerkleDAGOnly(cid);

                    if(map != null){
                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom(map.toByteArray())));
                    }else{
                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom("Not Found".getBytes())));
                    }

                    stream.writeAndFlush(builder.build());
                break;
            }
            case RESPONSE: {
                Dht.Record rcd = msg.getRecord();
                System.out.println("***:COME!!****val: " + rcd.getValue().toStringUtf8());
                break;
            }
            case ADD_PROVIDER: {
                List<Dht.Message.Peer> providers = msg.getProviderPeersList();
                byte[] remotePeerIdBytes = source.getBytes();
                Multihash hash = Multihash.deserialize(msg.getKey().toByteArray());
                if (providers.stream().allMatch(p -> Arrays.equals(p.getId().toByteArray(), remotePeerIdBytes))) {
                    providers.stream().map(PeerAddresses::fromProtobuf).forEach(p -> providersStore.addProvider(hash, p));
                }
                break;
            }
            case GET_PROVIDERS: {
                Set<PeerAddresses> allProviders = new HashSet<PeerAddresses>();

                Multihash hash = Multihash.deserialize(msg.getKey().toByteArray());
                Set<PeerAddresses> providers = providersStore.getProviders(hash);
                Cid cid = Cid.build(1, Cid.Codec.Raw, hash);

                PeerAddresses addrs = Kad.getIns().getOwnAddresses();

                //ipnsRecordにcidがあるかどうかチェックする．
                Optional<IpnsRecord> ret = ((DatabaseRecordStore) this.ipnsStore).getByValue("/ipfs/" + cid);
                if (!ret.isEmpty()) {
                    //DBにあれば，自身を追加する．
                    allProviders.add(addrs);
                }

                allProviders.addAll(providers);

                Dht.Message.Builder builder = msg.toBuilder();
                builder = builder.addAllProviderPeers(allProviders.stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray())
                        .stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));

                stream.writeAndFlush(builder.build());
                break;
            }
            case FIND_NODE: {
                Dht.Message.Builder builder = msg.toBuilder();
                builder = builder.addAllCloserPeers(getKClosestPeers(msg.getKey().toByteArray())
                        .stream()
                        .map(PeerAddresses::toProtobuf)
                        .collect(Collectors.toList()));
                stream.writeAndFlush(builder.build());
                break;
            }
            case PING: {
                break;
            } // Not used any more
            default:
                throw new IllegalStateException("Unknown message kademlia type: " + msg.getType());
        }
    }


}
