package org.peergos.protocol.dht;

import com.google.protobuf.*;
import crypto.pb.*;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.*;
import io.libp2p.core.crypto.*;
import org.ncl.kadrtt.core.AttrBean;
import org.ncl.kadrtt.core.Kad;
import org.peergos.*;
import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.*;
import org.peergos.protocol.ipns.pb.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

public interface KademliaController {

    CompletableFuture<Dht.Message> rpc(Dht.Message msg);

    CompletableFuture<Boolean> send(Dht.Message msg);

    default CompletableFuture<List<PeerAddresses>> closerPeers(Multihash peerID) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_NODE)
                .setKey(ByteString.copyFrom(peerID.toBytes()))
                .build())
                .thenApply(resp -> resp.getCloserPeersList().stream()
                        .map(PeerAddresses::fromProtobuf)
                        .collect(Collectors.toList()));
    }

    default CompletableFuture<Boolean> provide(Multihash block, PeerAddresses us) {
        return send(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.ADD_PROVIDER)
                // only provide the bare Multihash
                .setKey(ByteString.copyFrom(block.bareMultihash().toBytes()))
                .addAllProviderPeers(List.of(us.toProtobuf()))
                .build());
    }

    default CompletableFuture<Providers> getProviders(Multihash block) {
        return rpc(Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_PROVIDERS)
                .setKey(ByteString.copyFrom(block.bareMultihash().toBytes()))
                .build())
                .thenApply(Providers::fromProtobuf);
    }

    default CompletableFuture<Boolean> putAttr(byte[] rawData, String pathToPublish, LocalDateTime expiry, long sequence,
                                                long ttlNanos, Multihash peerId, PrivKey ourKey) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(rawData, true, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }

    default CompletableFuture<Boolean> putAttr(byte[] rawData, String pathToPublish, LocalDateTime expiry, long sequence,
                                               long ttlNanos, Multihash peerId, PrivKey ourKey, PeerAddresses pred, PeerAddresses suc) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        String in_pred = null;
        String in_suc = null;
        if(pred != null){
            in_pred = pred.toString();
        }
        if(suc != null){
            in_suc = suc.toString();
        }
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(rawData,in_pred,in_suc, true, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }


    default CompletableFuture<Boolean> putAttrInfo(byte[] rawData, boolean isProviderAddress, String pathToPublish, LocalDateTime expiry, long sequence,
                                                          long ttlNanos, Multihash peerId, PrivKey ourKey, PeerAddresses addr, String cid) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForAttr(rawData,addr, true, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos, cid);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }

    default CompletableFuture<Boolean> putTagInfo(String key, String value, String tagInfo, String tagCid,String pathToPublish, LocalDateTime expiry, long sequence,
                                                   long ttlNanos, Multihash peerId, PrivKey ourKey, PeerAddresses addr, String cid) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForTag(key, value, tagInfo, tagCid,addr,  pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos, cid);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }



    default CompletableFuture<Boolean> putProviderAddress(byte[] rawData, boolean isProviderAddress, String pathToPublish, LocalDateTime expiry, long sequence,
                                               long ttlNanos, Multihash peerId, PrivKey ourKey, PeerAddresses addr) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(rawData,addr, true, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }


    default CompletableFuture<Boolean> putValueWithAttr( List<HashMap<String,String>> attrList, List<HashMap<String, String>> tagList, LinkedList<Cid> cidList, String pathToPublish, LocalDateTime expiry, long sequence,
                                               long ttlNanos, Multihash peerId, PrivKey ourKey, String ex) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForAttrPut( attrList, tagList, cidList, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos, ex);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        //return putValue(peerId, ipnsEntry);
        return putMerkleDAG(peerId, ipnsEntry);
    }


    default CompletableFuture<Boolean> putValueForAdd(byte[] rawData, LinkedList<Cid> cidList, String pathToPublish, LocalDateTime expiry, long sequence,
                                                      long ttlNanos, Multihash peerId, PrivKey ourKey, String ex) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForAdd(rawData, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos, cidList, ex);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putMerkleDAG(peerId, ipnsEntry);
    }

    default CompletableFuture<Boolean> putRemainedChunk(byte[] rawData,  String pathToPublish, LocalDateTime expiry, long sequence,
                                                      long ttlNanos, Multihash peerId, PrivKey ourKey) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntrySingle(rawData, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }




    default CompletableFuture<Boolean> putValue(byte[] rawData, String pathToPublish, LocalDateTime expiry, long sequence,
                                                long ttlNanos, Multihash peerId, PrivKey ourKey) {

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(rawData, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }


    default CompletableFuture<Boolean> putValue(String pathToPublish, LocalDateTime expiry, long sequence,
                                                long ttlNanos, Multihash peerId, PrivKey ourKey) {

        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();

        return putValue(peerId, ipnsEntry);
    }



    default CompletableFuture<Boolean> putValue(Multihash peerId, byte[] value) {
        byte[] ipnsRecordKey = IPNS.getKey(peerId);
        System.out.println("IPNS Key:" + ipnsRecordKey.toString());
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.PUT_VALUE)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(value))
                        .build())
                .build();


//record-value

        return rpc(outgoing).thenApply(reply -> reply.getKey().equals(outgoing.getKey()) &&
                reply.getRecord().getValue().equals(outgoing.getRecord().getValue()));
    }


    default CompletableFuture<Boolean> putMerkleDAG(Multihash peerId, byte[] value) {
        byte[] ipnsRecordKey = IPNS.getKey(peerId);
        System.out.println("IPNS Key:" + ipnsRecordKey.toString());
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.PUT_MERKLEDAG)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(value))
                        .build())
                .build();


//record-value

        return rpc(outgoing).thenApply(reply -> reply.getKey().equals(outgoing.getKey()) &&
                reply.getRecord().getValue().equals(outgoing.getRecord().getValue()));
    }



    default CompletableFuture<GetResult> deliverValue(Dht.Message msg){
        System.out.println();
        ByteString bStr = msg.getRecord().getValue();
        String str = bStr.toStringUtf8();
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.RESPONSE)
                .setKey(msg.getRecord().getKey())
                .setRecord(msg.getRecord())
                .build();
        return rpc(outgoing).thenApply(GetResult::fromProtobuf);

    }



    default CompletableFuture<GetResult> getValue(Multihash peerId) {
        byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE)
                .setKey(ByteString.copyFrom(ipnsRecordKey))

                .build();
        return rpc(outgoing).thenApply(GetResult::fromProtobuf);
    }

  //  default CompletableFuture<GetResult> getValue2(String cid) {
   // default CompletableFuture<byte[]> getValue2(String cid) {
 //  default CompletableFuture<IpnsRecord> getValue2(String cid) {
    default CompletableFuture<Dht.Record> getValue2(String cid){
        //byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE)
                .setKey(ByteString.copyFrom(cid.getBytes()))
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
       // return rpc(outgoing).thenApply(GetResult::fromProtobuf);
        //return rpc(outgoing).thenApply(reply -> reply.getRecord().getValue().toByteArray());
       // return rpc(outgoing).thenApply(reply->reply.getRecord())
       // return rpc(outgoing).thenApply(IpnsRecord::fromProtobuf);
    }

    /**
     * チャンク取得用メソッド
     * @param cid
     * @return
     */
    default CompletableFuture<Dht.Record> getChunk(String cid){
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_CHUNK)
                .setKey(ByteString.copyFrom(cid.getBytes()))
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
    }

    /**
     * 属性の担当ノードかどうかを尋ねるメソッド
     * @param mask
     * @return
     */
    default CompletableFuture<Dht.Record> askInCharge(String mask){
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.INCHARGE)
                .setKey(ByteString.copyFrom(mask.getBytes()))
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
    }

    default CompletableFuture<Dht.Record> findMgr(String mask){
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.FIND_MGR)
                .setKey(ByteString.copyFrom(mask.getBytes()))
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
    }


    default CompletableFuture<Dht.Record> getMerkleDAG(String cid){
        //byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_MERKLEDAG)
                .setKey(ByteString.copyFrom(cid.getBytes()))
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
        // return rpc(outgoing).thenApply(GetResult::fromProtobuf);
        //return rpc(outgoing).thenApply(reply -> reply.getRecord().getValue().toByteArray());
        // return rpc(outgoing).thenApply(reply->reply.getRecord())
        // return rpc(outgoing).thenApply(IpnsRecord::fromProtobuf);
    }


    /**
     * 属性値の開始位置としてのcid (例: cid(time^08))を指定する．
     * さらに，範囲検索であるというフラグを立てる．
     * さらに，値の範囲を指定できる必要がある．
     * @param pathToPublish
     * @return
     */
    default CompletableFuture<Dht.Record> getValueByAttrMask(HashMap<String, Cborable> mapMap, String pathToPublish, String attrName, String attrCurrent, String attrMax, LocalDateTime expiry, long sequence,
                                                         long ttlNanos, Multihash peerId, PrivKey ourKey, boolean isCidOnly){
        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForTransfer(mapMap, pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos, attrName, attrCurrent, attrMax, isCidOnly);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();
//
        //   return putValue(peerId, ipnsEntry);
        byte[] ipnsRecordKey = IPNS.getKey(peerId);

        //byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE_WITH_ATTRS)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(ipnsEntry))
                        .build())
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
        // return rpc(outgoing).thenApply(GetResult::fromProtobuf);
        //return rpc(outgoing).thenApply(reply -> reply.getRecord().getValue().toByteArray());
        // return rpc(outgoing).thenApply(reply->reply.getRecord())
        // return rpc(outgoing).thenApply(IpnsRecord::fromProtobuf);
    }

    /**
     * 属性値の開始位置としてのcid (例: cid(time^08))を指定する．
     * さらに，範囲検索であるというフラグを立てる．
     * さらに，値の範囲を指定できる必要がある．
     * @param pathToPublish
     * @return
     */
    default CompletableFuture<Dht.Record> getValueByAttr(String pathToPublish, String attrName, String attrCurrent, String attrMax, LocalDateTime expiry, long sequence,
                                                         long ttlNanos, Multihash peerId, PrivKey ourKey, boolean isCidOnly){
        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntry(pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos, attrName, attrCurrent, attrMax, isCidOnly);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();
//
     //   return putValue(peerId, ipnsEntry);
        byte[] ipnsRecordKey = IPNS.getKey(peerId);

        //byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE_WITH_ATTRS)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(ipnsEntry))
                        .build())
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
        // return rpc(outgoing).thenApply(GetResult::fromProtobuf);
        //return rpc(outgoing).thenApply(reply -> reply.getRecord().getValue().toByteArray());
        // return rpc(outgoing).thenApply(reply->reply.getRecord())
        // return rpc(outgoing).thenApply(IpnsRecord::fromProtobuf);
    }

    default CompletableFuture<Dht.Record> getCidByTags(String pathToPublish, String tagName, String tagValue,
                                                       LocalDateTime expiry, long sequence,
                                                         long ttlNanos, Multihash tagCid, PrivKey ourKey){
        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = IPNS.createCborDataForIpnsEntryForTagRequest(tagCid.toString(),pathToPublish, expiry,
                Ipns.IpnsEntry.ValidityType.EOL_VALUE, sequence, ttlNanos);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();
//
        //   return putValue(peerId, ipnsEntry);
        byte[] ipnsRecordKey = IPNS.getKey(tagCid);

        //byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_CID_WITH_TAGS)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(ipnsEntry))
                        .build())
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);

    }


    default CompletableFuture<Boolean> registerMgr(SortedMap<String, Cborable> state, /*String pathToPublish, */
                                                   LocalDateTime expiry, long sequence,
                                                   long ttlNanos, Multihash peerId, PrivKey ourKey){

        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．
        byte[] cborEntryData = CborObject.CborMap.build(state).serialize();

        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                //.setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();
        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        Multihash randomPeerId = new Multihash(Multihash.Type.sha2_256, hash);
        byte[] ipnsRecordKey = IPNS.getKey(randomPeerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.REGISTER_MGR)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(ipnsEntry))
                        .build())
                .build();


        /*byte[] cborEntryData = CborObject.CborMap.build(state).serialize();

        byte[] hash = new byte[32];
        new Random().nextBytes(hash);
        Multihash randomPeerId = new Multihash(Multihash.Type.sha2_256, hash);
            byte[] ipnsRecordKey = IPNS.getKey(randomPeerId);
            Dht.Message outgoing = Dht.Message.newBuilder()
                    .setType(Dht.Message.MessageType.REGISTER_MGR)
                    //.setKey(ByteString.copyFrom(ipnsRecordKey))
                    .setRecord(Dht.Record.newBuilder()
                            //.setKey(ByteString.copyFrom(ipnsRecordKey))
                            .setValue(ByteString.copyFrom(cborEntryData))
                            .build())
                    .build();

         */
      // return rpc(outgoing).thenApply(GetResult::getRecord);


           return rpc(outgoing).thenApply(reply -> reply.getKey().equals(outgoing.getKey()) &&
                    reply.getRecord().getValue().equals(outgoing.getRecord().getValue()));


        }

    /**
     * 担当ノード -> Leafへのデータ取得
     * @param pathToPublish
     * @param map
     * @param attrInfo
     * @param expiry
     * @param sequence
     * @param ttlNanos
     * @param peerId
     * @param ourKey
     * @return
     */
    default CompletableFuture<Dht.Record> getValueByAttrMask(String pathToPublish, HashMap<String, AttrBean> map, String attrInfo, LocalDateTime expiry,
                                                             long sequence,
                                                             long ttlNanos, Multihash peerId, PrivKey ourKey, boolean isCidOnly){
        //ipnsEntry (byte[])から，getDataして，さらに，そこからmapでRawDataのキーで取ってくることができれば良い．

       // LinkedList<CborObject.CborMap> reqList = new LinkedList<CborObject.CborMap>();
        TreeMap<String, Cborable> globalReqMap = new TreeMap<String, Cborable>();


       Iterator<AttrBean> bIte = map.values().iterator();

     /*  while(bIte.hasNext()){
           AttrBean bean = bIte.next();
           //Map<String, Cborable> reqMap = IPNS.createCborMapForAttr(bean.getPeerId(), bean.getCid(), bean.getAttrMask(), bean.getAddr());
           //byte[] reqMap = IPNS.createCborMapForAttr(bean.getPeerId(), bean.getCid(), bean.getAttrMask(), bean.getAddr());
           globalReqMap.put(bean.getCid(), CborObject.CborMap.build(reqMap));

       }*/
      // globalReqMap.put("cidonly", new CborObject.CborBoolean(isCidOnly));
        //return CborObject.CborMap.build(state).serialize();



       // byte[] cborEntryData = CborObject.CborMap.build(globalReqMap).serialize();
        byte[] cborEntryData = IPNS.createCborMapForAttr(isCidOnly, map);
        String expiryString = IPNS.formatExpiry(expiry);
        byte[] signature = ourKey.sign(IPNS.createSigV2Data(cborEntryData));
        PubKey pubKey = ourKey.publicKey();
        byte[] pubKeyProtobuf = Crypto.PublicKey.newBuilder()
                .setType(pubKey.getKeyType())
                .setData(ByteString.copyFrom(pubKey.raw()))
                .build()
                .toByteArray();
        byte[] ipnsEntry = Ipns.IpnsEntry.newBuilder()
                .setSequence(sequence)
                .setTtl(ttlNanos)
                .setValue(ByteString.copyFrom(pathToPublish.getBytes()))
                .setValidityType(Ipns.IpnsEntry.ValidityType.EOL)
                .setValidity(ByteString.copyFrom(expiryString.getBytes()))
                .setData(ByteString.copyFrom(cborEntryData))
                .setSignatureV2(ByteString.copyFrom(signature))
                .setPubKey(ByteString.copyFrom(pubKeyProtobuf)) // not needed with Ed25519
                .build().toByteArray();
//
        //   return putValue(peerId, ipnsEntry);
        byte[] ipnsRecordKey = IPNS.getKey(peerId);

        //byte[] ipnsRecordKey = IPNS.getKey(peerId);
        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.GET_VALUE_AT_LEAF)
                .setKey(ByteString.copyFrom(ipnsRecordKey))
                .setRecord(Dht.Record.newBuilder()
                        .setKey(ByteString.copyFrom(ipnsRecordKey))
                        .setValue(ByteString.copyFrom(ipnsEntry))
                        .build())
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
    }


    default CompletableFuture<Dht.Record> checkPrivate(){

        Dht.Message outgoing = Dht.Message.newBuilder()
                .setType(Dht.Message.MessageType.QUERY_SWAM_KEY)
                //.setKey(ByteString.copyFrom(cid.getBytes()))
                .build();
        return rpc(outgoing).thenApply(GetResult::getRecord);
    }



}
