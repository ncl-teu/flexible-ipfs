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
            addressBook.addAddrs(peer, 0, addr);
            router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));

        }


    }

    public void addIncomingConnection(PeerId peer, Multiaddr addr) {
        if (Kad.getIns().isPeerExist(peer, addr)) {
            addressBook.addAddrs(peer, 0, addr);
            router.touch(Instant.now(), new Node(Id.create(Hash.sha256(peer.getBytes()), 256), peer.toString()));


        }

    }

    public List<PeerAddresses> getKClosestPeers(byte[] key) {
        int k = Kad.k;
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
     * @param attrName 属性名
     * @param attrCurrent 最小値（現状値）
     * @param attrMax 最大値
     * @return <CID, CborMapのデータ>
     **/
    public HashMap<String, Cborable> processAttrTransfer(HashMap<String, Cborable> mapMap,
                                                                      String attrName,
                                                                      String attrCurrent,
                                                                      String attrMax,
                                                                      boolean isCidOnly){
        //属性名^現状値　を生成
        String attrInfo = Kad.genAttrMask(attrName, attrCurrent);
       // System.out.println("*****"+attrInfo+"*****");

        String attrInfoMax = Kad.genAttrMask(attrName, attrMax);
        //DBから，time^08をキーにして，DBから文字列 (time^08)とaddr, cid を取得する．
        //そして，dialPeerでaddrとcidを渡して情報を検索する．
        //<PeerID, <CID, bean(attrmask, addr)>
        //指定の属性情報で，Leafノードを検索する．
        //<CID(time^08>, Map<コンテンツCID, bean>>という形式．
        //HashMap<String, HashMap<String, AttrBean>> attrMap = Kad.java.getIns().getStore().getAttrLink(attrInfo);

        //<コンテンツCID, Bean>のリストを取得．

        LinkedList<AttrBean> attrList = Kad.getIns().getStore().getAttrLink(attrInfo);
        //もしなければ，ここで打ち止め
        if(attrList.isEmpty()){
            //return null;
        }else{
        }
        CborObject.CborMap map = null;
        //あとは，peerIdごとに問い合わせるためのループ
        //HashMap<CID, Cbor>

        int sequence = 1;
        //cid = cid(time^08), str_cid
        //hashに近い指定数ピアを取得する．
        List<IpnsRecord> candidates = new ArrayList<>();
        List< CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
        //HashMap<String, Cborable> mapMap = new HashMap<String, Cborable>();
        long ttl = 1 * 3600_000_000_000L;

        LocalDateTime expiry = LocalDateTime.now().plusHours(1);

        //CID(time^val)たち
        //Iterator<String> pIte = attrMap.keySet().iterator();
        Iterator<AttrBean> pIte = attrList.iterator();
        HashSet<String> getSet = new HashSet<String>();

        HashMap<String, byte[]> retMap = new HashMap<String, byte[]>();
        //<CID, bean>のループ
        while(pIte.hasNext()){
            AttrBean bean = pIte.next();
            //CIDを取得．
            String contentCID = bean.getAddr();
            mapMap.put(contentCID, new CborObject.CborString(contentCID));


        }

        if(attrCurrent.equals(attrMax)){
            //この時点でmax値であれば，ここで打ち止め．
            //集約したデータを返す．

        }else{
            //Sucとなる担当ノードのエンドポイントを取得する．
            int nextVal = Integer.valueOf(attrCurrent).intValue() + 1;
            //String nextAttr = Kad.java.genAttrMask(attrName, nextVal);
           // String nextAttr = Kad.java.genNormalizedValue(nextVal);
            String nextAttr = Kad.genNormalizedValue(attrName, nextVal);
            HashMap<String ,String> sucMap = Kad.getIns().getStore().getSuc(attrInfo);
            String sucPeerAddress = sucMap.get("suc");
            if(sucPeerAddress == null){
                //レコードが無い場合は，無視
                //他のピアへ転送する．

            }else{
                String succid = sucMap.get("cid");
                PeerAddresses sucAddr = Kad.genPeerAddresses(sucPeerAddress);
                //DBから，SUCの担当ノードを特定する．
                //SUCの担当ノードが，また自分かどうかのチェックが必要．
                if(Kad.isOwnHost(sucAddr)){
                    //sucAddr, 属性名，属性情報, 最大値が必要
                    //GET_VALUE_WITH_ATTRSのメッセージをもって呼び出す．
                    HashMap<String, Cborable> recursiveMap = this.processAttrTransfer(mapMap, attrName, nextAttr, attrMax, isCidOnly);
                    if(recursiveMap.isEmpty()){

                    }else{
                        Iterator<String> keyIte = recursiveMap.keySet().iterator();

                        mapMap.putAll(recursiveMap);
                    }


                }else{
                    try{

                        //SUCが他ノードであれば，リクエストを転送する．
                        CompletableFuture<List<CborObject.CborMap>> retList2 = Kad.getIns().getKadDHT().
                                getValueByAttrs2(mapMap, Kad.genCid(succid), sucAddr, attrName, nextAttr, attrMax, Kad.getIns().node, isCidOnly);
                        List<CborObject.CborMap> mList = retList2.get();
                        Iterator<CborObject.CborMap> mIte = mList.iterator();


                        while(mIte.hasNext()){
                            CborObject.CborMap m = mIte.next();
                            /*if(m.containsKey("cid")){
                                String ccid = ((CborObject.CborString)m.get("cid")).value;
                                mapMap.put(ccid, new CborObject.CborString(ccid));
                            }
                        */
                            Iterator<String> keyIte = m.keySet().iterator();
                            while(keyIte.hasNext()){
                                String key = keyIte.next();
                                mapMap.put(key, m.get(key));
                            }
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

                        //isprovierputは使わなくなった．
                        }else if(map.containsKey("isproviderput")) {
                            //プロバイダのアドレス情報PUTの場合，DBへ登録する．
                            //属性情報を取得する（例: time^08)
                            CborObject.CborByteArray valarray = (CborObject.CborByteArray) map.get("RawData");
                            String val = new String(valarray.value);

                            CborObject.CborString addr = (CborObject.CborString) map.get("addr");
                            Kad.getIns().getStore().putAttrLink(cid, val, addr.value);
                        //こちらが本来の方式．CIDを担当ノードの属性のところへ登録する．
                        }else if(map.containsKey("isprovidercidput")){
                            CborObject.CborByteArray valarray = (CborObject.CborByteArray) map.get("RawData");
                            String val = new String(valarray.value);
                            CborObject.CborString attr_content_cid_cbor = (CborObject.CborString) map.get("cid");
                            //CborObject.CborString addr = (CborObject.CborString) map.get("addr");

                            //Multihash, MultiAddressのリストの形式
                            //12D3KooWA5pL6SFauCsKqJK7J8dzXSUGqNbSQLNQ7PkaDihvv1ZG: [/ip4/60.112.207.90/tcp/52456, /ip4/60.112.207.90/tcp/52692]
                            //String bAddr = new String(addr.value);
                            //PeerAddresses pAddr = Kad.java.genPeerAddresses(bAddr);
                            //val (time^08), pAddr.toString()を登録する．
                            //cid(time^08), time^08, prividerのaddr
                            Kad.getIns().getStore().putAttrLink(cid, val, attr_content_cid_cbor.value);

                        }else if(map.containsKey("istagput")){
                            //Tagのputの場合の処理
                            CborObject.CborString ctagcid = (CborObject.CborString) map.get("tagcid");
                            String tagCid = ctagcid.value;

                            CborObject.CborString attr_content_cid_cbor = (CborObject.CborString) map.get("cid");
                            String content_cid = attr_content_cid_cbor.value;

                            CborObject.CborString ctagName = (CborObject.CborString)map.get("tagname");
                            String tagName = ctagName.value;

                            CborObject.CborString ctagValue = (CborObject.CborString)map.get("tagvalue");
                            String tagValue = ctagValue.value;

                            CborObject.CborString ctagInfo = (CborObject.CborString) map.get("taginfo");
                            String tagInfo = ctagInfo.value;
                            //Tag情報をDBに登録する．
                            Kad.getIns().getStore().putTagInfo(tagCid, tagInfo, tagName, tagValue, content_cid);

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

            case GET_CID_WITH_TAGS:
            {
                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);
                if (mapping != null) {
                    try {

                        HashMap<String, Cborable> mapMap = new HashMap<String, Cborable>();
                        //ipnsStore.put(mapping.get().publisher, mapping.get().value);
                        //cborに格納されている各種値を取得する．
                        Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                        CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                        CborObject.CborMap map = (CborObject.CborMap) cbor;
                        CborObject.CborString cbortagCid = (CborObject.CborString) map.get("tagcid");
                        String tagCid = cbortagCid.value;
                        HashSet<String> tags = Kad.getIns().getStore().getTagInfo(tagCid);
                        Iterator<String> tagIte = tags.iterator();
                        while(tagIte.hasNext()){
                            String str = tagIte.next();
                            mapMap.put(str, new CborObject.CborString(str));
                        }
                        //HashMap<String, Cborable> allMap = Kad.java.getIns().getKadDHT().getAttrInfoAgain(mapMap, source, attrCid, attrName, attrCurrent, attrMax, us);

                        byte[] retData = CborObject.CborMap.build(mapMap).serialize();
                        Dht.Message.Builder builder = msg.toBuilder();
                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom(retData)));
                        stream.writeAndFlush(builder.build());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
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

                //Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);
                if (true) {
                    try{
                        Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                        CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                        CborObject.CborMap map = (CborObject.CborMap) cbor;
                        //mapが，CborMapのこと．
                        //<cid, CborMap>という形式
                        Iterator<String> cIte = map.keySet().iterator();
                        LinkedList<Cborable> retList = new LinkedList<Cborable>();
                        //CborObject.CborBoolean isCborOnlyC = (CborObject.CborBoolean) map.get("cidonly");
                       //boolean isCidOnly = isCborOnlyC.value;

                        //cidに対するループ
                        while(cIte.hasNext()){
                            String cid = cIte.next();
                            //CborObject.CborMap m = (CborObject.CborMap)map.get(cid);
                            CborObject.CborMap  m = (CborObject.CborMap)map.get(cid);

                            //cidについて，ディレクトリ＋DBに双方存在すればOK．
                            //あれば，ディレクトリを参照してcbor化する．
                            //まずはピアID
                            CborObject.CborString c_peerid = (CborObject.CborString) m.get("peerid");
                            String peerid = c_peerid.value;

                            CborObject.CborString c_cid = (CborObject.CborString) m.get("cid");
                            //属性情報（例: time^08)
                            CborObject.CborString c_attrmask = (CborObject.CborString) m.get("attrmask");
                            String attrmask = c_attrmask.value;
                            //この，問い合されたノードのピア接続先情報
                            CborObject.CborString c_addr = (CborObject.CborString) m.get("addr");
                            String addr = c_addr.value;

                            retList = Kad.getContentCidFromAttr(attrmask);

                            //まずはファイル存在チェック
                      /*      if(Kad.java.isDAGExists(cid)){
                                Kad.java.getContentCidFromAttr(attrmask);
                                //次に，DBを見る．本来は見るべきだが，もうあるという前提で，返送する．
                                //他の属性についてはすでにcByteに含まれているはず．
                                //byte[] cByte = Kad.java.getDataFromMerkleDAG(cid);
                                //あとは，Mapに設定するのみ．
                                CborObject.CborMap retMap = Kad.java.readMerkleDAGOnly(cid);

                                if(retMap == null){

                                }else{
                                    retMap.put("cid", new CborObject.CborString(cid));



                                    retList.add(retMap);
                                }


                            }else{
                                continue;
                            }
*/
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
               /*a String str_swarm = Kad.java.getIns().getSwarmKey();

                Dht.Message.Builder builder = msg.toBuilder();

                builder = builder.setRecord(Dht.Record.newBuilder()
                        .setKey(msg.getKey())
                        //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                        .setValue(ByteString.copyFrom(str_swarm.getBytes())));
                stream.writeAndFlush(builder.build());

                */
                break;
            }
            case REGISTER_MGR:{
                try{
                    Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
                    CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
                    //if (! (cbor instanceof CborObject.CborMap))
                    CborObject.CborMap map = (CborObject.CborMap) cbor;
                    Iterator<String> kIte = map.keySet().iterator();
                    while(kIte.hasNext()){
                        //属性名^値のループ
                        String mask = kIte.next();
                        CborObject.CborMap inMap = (CborObject.CborMap)map.get(mask);
                        CborObject.CborString cbor_attrName = (CborObject.CborString) map.get("attrName");

                        String maskcid = ((CborObject.CborString)inMap.get("maskcid")).value;
                        String addr = ((CborObject.CborString)inMap.get("addr")).value;
                        //そして登録する．
                        Kad.getIns().getStore().putMgr(mask, maskcid, addr);


                    }
                    CborObject.CborBoolean ret = new CborObject.CborBoolean(true);

                    Dht.Message.Builder builder = msg.toBuilder();
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            .setValue(ByteString.copyFrom(ret.toByteArray())));

                    stream.writeAndFlush(builder.build());
                }catch(Exception e){
                    e.printStackTrace();
                }

                break;
            }

            //担当ノードに対して指定の属性情報で問い合わせが来たとき
                //<CID, Cborble/CborMap>を返す．

            case GET_VALUE_WITH_ATTRS: {
                Optional<IpnsMapping> mapping = IPNS.validateIpnsEntry(msg);

                if (mapping != null) {
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
                        //CborObject mapMapCbor = CborObject.fromByteArray(map.get("mapMap").toCbor().toByteArray());
                       // CborObject.CborMap mapMaporg = (CborObject.CborMap)mapMapCbor;
                        //CborObject.CborMap mapMaporg = map.get("mapMap");


                        CborObject.CborString cbor_attrName = (CborObject.CborString) map.get("attrName");
                        String attrName = cbor_attrName.value;
                        map.remove(new CborObject.CborString("attrName"));
                        CborObject.CborString cbor_attrCurrent = (CborObject.CborString) map.get("attrCurrent");
                        String attrCurrent = cbor_attrCurrent.value;
                        map.remove(new CborObject.CborString("attrCurrent"));

                        CborObject.CborString cbor_attrMax = (CborObject.CborString) map.get("attrMax");
                        String attrMax = cbor_attrMax.value;
                        map.remove(new CborObject.CborString("attrMax"));
                        boolean isCidOnly = false;

                        CborObject.CborBoolean isCidOnlyC = (CborObject.CborBoolean) map.get("cidonly");
                        if(isCidOnlyC.value){
                            isCidOnly = true;
                        }
                        map.remove(new CborObject.CborString("cidonly"));
                        //そもそも当該ノードが，arrtName^attrCurrentの担当ノードなのかどうかをチェックする必要がある．

                        HashMap<String, Cborable> mapMap = new HashMap<String, Cborable>();
                        //Iterator<String> keyIte = mapMaporg.keySet().iterator();
                        Iterator<String> keyIte = map.keySet().iterator();
                        while(keyIte.hasNext()){
                            String ccid = keyIte.next();
                            mapMap.put(ccid, map.get(ccid));
                        }

                        String attrMask = Kad.genAttrMask(attrName, attrCurrent);
                        if(Kad.getIns().getStore().isInCharge(attrMask)){
                            //復号化した後の処理は関数化可能．
                            HashMap<String, Cborable> allMap = this.processAttrTransfer(mapMap, attrName, attrCurrent, attrMax, isCidOnly);
                            allMap.putAll(mapMap);

                            byte[] retData = CborObject.CborMap.build(allMap).serialize();
                            Dht.Message.Builder builder = msg.toBuilder();
                            builder = builder.setRecord(Dht.Record.newBuilder()
                                    .setKey(msg.getKey())
                                    //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                    .setValue(ByteString.copyFrom(retData)));
                            stream.writeAndFlush(builder.build());
                        }else{
                            //担当ノードではない場合は，他ノードへ移譲する．
                            Host us = Kad.getIns().getNode();

                            //DHTによる最短距離のノード集合を取得する．
                            Cid attrCid = Kad.genCid(attrMask);
                            //
                            HashMap<String, Cborable> allMap = Kad.getIns().getKadDHT().getAttrInfoAgain(mapMap, source, attrCid, attrName, attrCurrent, attrMax, us);
                            allMap.putAll(mapMap);
                            byte[] retData = CborObject.CborMap.build(allMap).serialize();
                            Dht.Message.Builder builder = msg.toBuilder();
                            builder = builder.setRecord(Dht.Record.newBuilder()
                                    .setKey(msg.getKey())
                                    //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                    .setValue(ByteString.copyFrom(retData)));
                            stream.writeAndFlush(builder.build());
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }


                    break;
                }
            }
            case FIND_MGR:{
                String mask = msg.getKey().toStringUtf8();
                Dht.Message.Builder builder = msg.toBuilder();
                String mgrAddr = Kad.getIns().getStore().getMgr(mask);
                if(mgrAddr != null){

                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                            .setValue(ByteString.copyFrom(new CborObject.CborString(mgrAddr).toByteArray())));
                }else{
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                            .setValue(ByteString.copyFrom(new CborObject.CborString("").toByteArray())));
                }
                stream.writeAndFlush(builder.build());

                break;
            }
            case INCHARGE: {
                //maskを取得する．
                String mask = msg.getKey().toStringUtf8();
                Dht.Message.Builder builder = msg.toBuilder();
                if(Kad.getIns().getStore().isInCharge(mask)){
                    CborObject.CborBoolean ret = new CborObject.CborBoolean(true);

                    //担当ノードであれば，OK
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                            .setValue(ByteString.copyFrom(ret.toByteArray())));
                }else{
                    CborObject.CborBoolean ret = new CborObject.CborBoolean(false);
                    //担当ノードであれば，OK
                    builder = builder.setRecord(Dht.Record.newBuilder()
                            .setKey(msg.getKey())
                            //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                            .setValue(ByteString.copyFrom(ret.toByteArray())));
                }
                stream.writeAndFlush(builder.build());


                break;
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

                stream.writeAndFlush(builder.build());
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
                        SortedMap<String, Cborable> state = new TreeMap<>();
                        map = CborObject.CborMap.build(state);

                        builder = builder.setRecord(Dht.Record.newBuilder()
                                .setKey(msg.getKey())
                                //.setValue(ByteString.copyFrom(ipnsRecord.get().raw)));
                                .setValue(ByteString.copyFrom(map.toByteArray())));
                    }

                    stream.writeAndFlush(builder.build());
                break;
            }
            case RESPONSE: {
                Dht.Record rcd = msg.getRecord();
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
