package org.peergos.protocol.ipns;

import com.google.protobuf.*;
import crypto.pb.*;
import io.ipfs.cid.*;
import io.ipfs.multihash.*;
import io.libp2p.core.crypto.*;
import io.libp2p.crypto.keys.*;
import org.ncl.kadrtt.core.AttrBean;
import org.peergos.PeerAddresses;
import org.peergos.cbor.*;
import org.peergos.protocol.dht.pb.*;
import org.peergos.protocol.ipns.pb.*;

import java.io.*;
import java.nio.charset.*;
import java.time.*;
import java.time.format.*;
import java.util.*;

public class IPNS {
    public static final int MAX_RECORD_SIZE = 10*1024;

    public static final DateTimeFormatter rfc3339nano = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.n");
    
    public static String formatExpiry(LocalDateTime expiry) {
        return expiry.atOffset(ZoneOffset.UTC).format(rfc3339nano)+"Z";
    }

    public static byte[] getKey(Multihash peerId) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write("/ipns/".getBytes());
            bout.write(peerId.toBytes());
        } catch (IOException e) {}
        return bout.toByteArray();
    }

    public static Cid getCidFromKey(ByteString key) {
        if (! key.startsWith(ByteString.copyFrom("/ipns/".getBytes(StandardCharsets.UTF_8))))
            throw new IllegalStateException("Unknown IPNS key space: " + key);
        ByteString subStr = key.substring(6);

        byte[] byteArr = subStr.toByteArray();
        Cid cid = Cid.cast(byteArr);
        return cid;

        //return Cid.cast(key.substring(6).toByteArray());
    }

    public static Cid getCidFromKey2(ByteString key) {
        if (! key.startsWith(ByteString.copyFrom("/ipns/".getBytes(StandardCharsets.UTF_8))))
            throw new IllegalStateException("Unknown IPNS key space: " + key);
        ByteString subStr = key.substring(6);
        byte[] byteArr = subStr.toByteArray();
        Cid cid = Cid.buildCidV1(Cid.Codec.Raw, Multihash.Type.id, byteArr);

        //Cid cidv0 = Cid.buildV0(cid);

        return cid;

        //return Cid.cast(key.substring(6).toByteArray());
    }

    public static byte[] createCborDataForIpnsEntry(String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        return CborObject.CborMap.build(state).serialize();
    }

    /**
     * Data部のバイナリ生成メソッド
     * H. Kanemitsu
     * @param rawData
     * @param pathToPublish
     * @param expiry
     * @param validityType
     * @param sequence
     * @param ttl
     * @return
     */
    public static byte[] createCborDataForIpnsEntry(byte[] rawData,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl
                                                    ) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        return CborObject.CborMap.build(state).serialize();
    }
    public static byte[] createCborDataForIpnsEntryForAdd(byte[] rawData,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl,
                                                    LinkedList<Cid> cidList,
                                                          String ex) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        state.put("extension", new CborObject.CborString(ex));

        Iterator<Cid> cIte = cidList.iterator();
        LinkedList<CborObject.CborString> cidStrList = new LinkedList<CborObject.CborString>();
        while(cIte.hasNext()){
            Cid ccid = cIte.next();
            cidStrList.add(new CborObject.CborString(ccid.toString()));
        }
        state.put("cidlist", new CborObject.CborList(cidStrList));

        return CborObject.CborMap.build(state).serialize();
    }

    public static byte[] createCborDataForIpnsEntrySingle(byte[] rawData,
                                                          String pathToPublish,
                                                          LocalDateTime expiry,
                                                          long validityType,
                                                          long sequence,
                                                          long ttl) {
        CborObject.CborByteArray array = new CborObject.CborByteArray(rawData);
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));


        /*state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        */

        return CborObject.CborMap.build(state).serialize();
    }






    /**
     * Data部のバイナリ生成メソッド
     * H. Kanemitsu
     * @param rawData
     * @param pathToPublish
     * @param expiry
     * @param validityType
     * @param sequence
     * @param ttl
     * @return
     */
    public static byte[] createCborDataForIpnsEntry(byte[] rawData,
                                                    boolean isAttrPut,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));
        state.put("isattrput", new CborObject.CborBoolean(true));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        return CborObject.CborMap.build(state).serialize();
    }

    public static byte[] createCborDataForIpnsEntryForAttrPut(
                                                    List<HashMap<String, String>> list,
                                                    List<HashMap<String, String>> tagList,
                                                    LinkedList<Cid> cidList,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl,String ex) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        Iterator<HashMap<String, String>> mIte = list.iterator();
        while(mIte.hasNext()){
            HashMap<String, String> map = mIte.next();
            Iterator<String> keyIte = map.keySet().iterator();
            while(keyIte.hasNext()){
                String key = keyIte.next();
                String value = map.get(key);
                state.put(key, new CborObject.CborString(value));
            }
        }
        Iterator<HashMap<String, String>> tIte = tagList.iterator();

        while(tIte.hasNext()){
            HashMap<String, String> map = tIte.next();
            Iterator<String> keyIte = map.keySet().iterator();
            while(keyIte.hasNext()){
                String key = keyIte.next();
                String value = map.get(key);
                state.put(key, new CborObject.CborString(value));
            }
        }

        //state.put("isattrput", new CborObject.CborBoolean(true));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        state.put("extension", new CborObject.CborString(ex));
        Iterator<Cid> cIte = cidList.iterator();
        LinkedList<CborObject.CborString> cidStrList = new LinkedList<CborObject.CborString>();
        while(cIte.hasNext()){
            Cid ccid = cIte.next();
            cidStrList.add(new CborObject.CborString(ccid.toString()));
        }
        state.put("cidlist", new CborObject.CborList(cidStrList));

        return CborObject.CborMap.build(state).serialize();
    }

    /**
     * 属性つきコンテンツのPUT用のバイナリデータ生成メソッド
     * @param rawData
     * @param list
     * @param pathToPublish
     * @param expiry
     * @param validityType
     * @param sequence
     * @param ttl
     * @return
     */
    public static byte[] createCborDataForIpnsEntry(byte[] rawData,
                                                    List<HashMap<String, String>> list,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        Iterator<HashMap<String, String>> mIte = list.iterator();
        while(mIte.hasNext()){
            HashMap<String, String> map = mIte.next();
            Iterator<String> keyIte = map.keySet().iterator();
            while(keyIte.hasNext()){
                String key = keyIte.next();
                String value = map.get(key);
                state.put(key, new CborObject.CborString(value));
            }
        }
        state.put("RawData", new CborObject.CborByteArray(rawData));
        //state.put("isattrput", new CborObject.CborBoolean(true));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        return CborObject.CborMap.build(state).serialize();
    }

    /**
     * 担当ノードへの属性情報PUTのためのデータ生成メソッド
     * @param rawData
     * @param pred
     * @param suc
     * @param isAttrPut
     * @param pathToPublish
     * @param expiry
     * @param validityType
     * @param sequence
     * @param ttl
     * @return
     */
    public static byte[] createCborDataForIpnsEntry(byte[] rawData,
                                                    String pred,
                                                    String suc,
                                                    boolean isAttrPut,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));
        if(pred != null){
            state.put("pred", new CborObject.CborString(pred));

        }
        if(suc != null){
            state.put("suc", new CborObject.CborString(suc));

        }
        state.put("isattrput", new CborObject.CborBoolean(isAttrPut));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));
        return CborObject.CborMap.build(state).serialize();
    }

    public static byte[] createCborDataForIpnsEntryForAttr(byte[] rawData,
                                                    PeerAddresses addr,
                                                    boolean isProviderPut,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl,String cid) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));
        state.put("cid", new CborObject.CborString(cid));
        // state.put("addr", new CborObject.CborByteArray(addr.toProtobuf().toByteArray()));
        state.put("addr", new CborObject.CborString(addr.toString()));
        state.put("isprovidercidput", new CborObject.CborBoolean(isProviderPut));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));

        return CborObject.CborMap.build(state).serialize();

    }



    public static byte[] createCborDataForIpnsEntryForTagRequest(
                                                          String tagcid,
                                                          String pathToPublish,
                                                          LocalDateTime expiry,
                                                          long validityType,
                                                          long sequence,
                                                          long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("tagcid", new CborObject.CborString(tagcid));
        // state.put("addr", new CborObject.CborByteArray(addr.toProtobuf().toByteArray()));
      //  state.put("addr", new CborObject.CborString(addr.toString()));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));

        return CborObject.CborMap.build(state).serialize();

    }



    public static byte[] createCborDataForIpnsEntryForTag(String key,
                                                          String value,
                                                          String tagInfo,
                                                          String tagCid,
                                                           PeerAddresses addr,
                                                           String pathToPublish,
                                                           LocalDateTime expiry,
                                                           long validityType,
                                                           long sequence,
                                                           long ttl,String cid) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("tagname", new CborObject.CborString(key));
        state.put("tagvalue", new CborObject.CborString(value));
        state.put("taginfo", new CborObject.CborString(tagInfo));
        state.put("tagcid", new CborObject.CborString(tagCid));
        state.put("cid", new CborObject.CborString(cid));
        // state.put("addr", new CborObject.CborByteArray(addr.toProtobuf().toByteArray()));
        //state.put("addr", new CborObject.CborString(addr.toString()));
        state.put("istagput", new CborObject.CborBoolean(true));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));

        return CborObject.CborMap.build(state).serialize();

    }


    public static byte[] createCborDataForIpnsEntry(byte[] rawData,
                                                    PeerAddresses addr,
                                                    boolean isProviderPut,
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("RawData", new CborObject.CborByteArray(rawData));
       // state.put("addr", new CborObject.CborByteArray(addr.toProtobuf().toByteArray()));
        state.put("addr", new CborObject.CborString(addr.toString()));
        state.put("isproviderput", new CborObject.CborBoolean(isProviderPut));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));

        return CborObject.CborMap.build(state).serialize();

    }

    /**
     * Leafに依頼するためのMapを生成する．
     *
     * @return
     */
   public static Map<String, Cborable> createCborMapForAttr(String peerid, String cid, String attrMask, String addr) {

        SortedMap<String, Cborable> state = new TreeMap<>();

        // state.put("addr", new CborObject.CborByteArray(addr.toProtobuf().toByteArray()));
        state.put("peerid", new CborObject.CborString(peerid));
        state.put("cid", new CborObject.CborString(cid));
        state.put("attrmask", new CborObject.CborString(attrMask));
        state.put("addr", new CborObject.CborString(addr));
        return state;
        //return CborObject.CborMap.build(state).serialize();

    }

    public static byte[] createCborMapForAttr(boolean isCidOnly, HashMap<String, AttrBean> map) {

        SortedMap<String, Cborable> state = new TreeMap<>();

        Iterator<AttrBean> bIte = map.values().iterator();

        while(bIte.hasNext()){
            AttrBean bean = bIte.next();
            SortedMap<String, Cborable> tmp = new TreeMap<>();
            //tmp.put("peerid", new CborObject.CborString(bean.getPeerId()));
            tmp.put("cid", new CborObject.CborString(bean.getCid()));
            tmp.put("attrmask", new CborObject.CborString(bean.getAttrMask()));
            tmp.put("addr", new CborObject.CborString(bean.getAddr()));


            //byte[] reqMap = IPNS.createCborMapForAttr(bean.getPeerId(), bean.getCid(), bean.getAttrMask(), bean.getAddr());
            state.put(bean.getCid(), CborObject.CborMap.build(tmp));

        }
        //state.put("cidonly", new CborObject.CborBoolean(isCidOnly));

        //return state;
        return CborObject.CborMap.build(state).serialize();

    }


    /**
     * 属性＋範囲指定クエリ用のCbor生成メソッド
     * @param pathToPublish
     * @param expiry
     * @param validityType
     * @param sequence
     * @param ttl
     * @param attrCurrent
     * @param attrMax
     * @return
     */
    public static byte[] createCborDataForIpnsEntryForTransfer(
            HashMap<String, Cborable> mapMap,
            String pathToPublish,
            LocalDateTime expiry,
            long validityType,
            long sequence,
            long ttl,
            String attrName,
            String attrCurrent,
            String attrMax,
            boolean isCidOnly) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        //state.put("mapMap", new CborObject.CborByteArray(CborObject.CborMap.build(mapMap).serialize()));
        Iterator<String> keyIte = mapMap.keySet().iterator();
        while(keyIte.hasNext()){
            String key = keyIte.next();
            state.put(key, new CborObject.CborString(key));
        }
        //state.put("mapMap", new CborObject.CborByteArray(mapMap.))
        state.put("attrName", new CborObject.CborString(attrName));
        state.put("cidonly", new CborObject.CborBoolean(isCidOnly));
        state.put("attrCurrent", new CborObject.CborString(attrCurrent));
        state.put("attrMax", new CborObject.CborString(attrMax));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));


        return CborObject.CborMap.build(state).serialize();

    }


    /**
     * 属性＋範囲指定クエリ用のCbor生成メソッド
     * @param pathToPublish
     * @param expiry
     * @param validityType
     * @param sequence
     * @param ttl
     * @param attrCurrent
     * @param attrMax
     * @return
     */
    public static byte[] createCborDataForIpnsEntry(
                                                    String pathToPublish,
                                                    LocalDateTime expiry,
                                                    long validityType,
                                                    long sequence,
                                                    long ttl,
                                                    String attrName,
                                                    String attrCurrent,
                                                    String attrMax,
                                                    boolean isCidOnly) {
        SortedMap<String, Cborable> state = new TreeMap<>();
        state.put("attrName", new CborObject.CborString(attrName));
        state.put("cidonly", new CborObject.CborBoolean(isCidOnly));
        state.put("attrCurrent", new CborObject.CborString(attrCurrent));
        state.put("attrMax", new CborObject.CborString(attrMax));
        state.put("TTL", new CborObject.CborLong(ttl));
        state.put("Value", new CborObject.CborByteArray(pathToPublish.getBytes()));
        state.put("Sequence", new CborObject.CborLong(sequence));
        String expiryString = formatExpiry(expiry);
        state.put("Validity", new CborObject.CborByteArray(expiryString.getBytes(StandardCharsets.UTF_8)));
        state.put("ValidityType", new CborObject.CborLong(validityType));


        return CborObject.CborMap.build(state).serialize();

    }



    public static byte[] createSigV2Data(byte[] data) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        try {
            bout.write("ipns-signature:".getBytes(StandardCharsets.UTF_8));
            bout.write(data);
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<IpnsMapping> validateIpnsEntry(Dht.Message msg) {
        if (! msg.hasRecord() || msg.getRecord().getValue().size() > IPNS.MAX_RECORD_SIZE)
            return Optional.empty();
        if (! msg.getKey().equals(msg.getRecord().getKey()))
            return Optional.empty();
        if (! msg.getRecord().getKey().startsWith(ByteString.copyFrom("/ipns/".getBytes(StandardCharsets.UTF_8))))
            return Optional.empty();
        byte[] cidBytes = msg.getRecord().getKey().substring(6).toByteArray();
        Multihash signer = Multihash.deserialize(cidBytes);
        try {
            Ipns.IpnsEntry entry = Ipns.IpnsEntry.parseFrom(msg.getRecord().getValue());
            if (! entry.hasSignatureV2() || ! entry.hasData())
                return Optional.empty();
            PubKey pub;
            if (signer.getType() == Multihash.Type.id) {
                byte[] pubKeymaterial = Arrays.copyOfRange(signer.getHash(), 4, 36);
                pub = new Ed25519PublicKey(new org.bouncycastle.crypto.params.Ed25519PublicKeyParameters(pubKeymaterial, 0));
            } else {
                Crypto.PublicKey publicKey = Crypto.PublicKey.parseFrom(entry.getPubKey());
                pub = RsaKt.unmarshalRsaPublicKey(publicKey.getData().toByteArray());
            }
           /* if (! pub.verify(ByteString.copyFrom("ipns-signature:".getBytes()).concat(entry.getData()).toByteArray(),
                    entry.getSignatureV2().toByteArray()))
                return Optional.empty();

            */
            CborObject cbor = CborObject.fromByteArray(entry.getData().toByteArray());
            if (! (cbor instanceof CborObject.CborMap))
                return Optional.empty();
            CborObject.CborMap map = (CborObject.CborMap) cbor;
            if (map.getLong("Sequence") != entry.getSequence())
                return Optional.empty();
            if (map.getLong("TTL") != entry.getTtl())
                return Optional.empty();
            if (map.getLong("ValidityType") != entry.getValidityType().getNumber())
                return Optional.empty();
            if (! Arrays.equals(map.getByteArray("Value"), entry.getValue().toByteArray()))
                return Optional.empty();
            byte[] validity = entry.getValidity().toByteArray();
            if (! Arrays.equals(map.getByteArray("Validity"), validity))
                return Optional.empty();
            LocalDateTime expiry = LocalDateTime.parse(new String(validity).substring(0, validity.length - 1), IPNS.rfc3339nano);
           /* if (expiry.isBefore(LocalDateTime.now()))
                return Optional.empty();*/
            byte[] entryBytes = msg.getRecord().getValue().toByteArray();
            IpnsRecord record = new IpnsRecord(entryBytes, entry.getSequence(), entry.getTtl(), expiry, entry.getValue().toStringUtf8());
            return Optional.of(new IpnsMapping(signer, record));
        } catch (InvalidProtocolBufferException e) {
            return Optional.empty();
        }
    }
}
