package org.ncl.kadrtt.core;

import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.peergos.Hash;
import org.peergos.PeerAddresses;
import org.peergos.cbor.CborObject;
import org.peergos.protocol.dht.DatabaseRecordStore;
import org.peergos.protocol.dht.Kademlia;
import org.peergos.protocol.dht.ProviderStore;
import org.peergos.protocol.dht.RamProviderStore;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Kad {

    private static Kad own;

    private static Properties prop;

    public static int KAD_MODE;

    public static int k;

    public static int alpha;


    public static int beta;

    public static boolean isPrivate;

    public static String swarmKey;

    public static LinkedList<String> peerList;

    public static String providerPath;

    public static String getdataPath;

    public ProviderStore providerStore;

    public Host node;

    public PeerAddresses ownAddresses;

    public String globalIP = null;

    public static int putRedundancy;

    public Kademlia kadDHT;

    public DatabaseRecordStore store;

    public static String delimiter = "_";

    public static String GW_ENDPOINT;


    private Kad(){

    }


    public void initialize(String fileName){
        try {
            Kad.peerList = new LinkedList<String>();
            Kad.prop = new Properties();
            Kad.prop.load(new FileInputStream(fileName));
            Kad.KAD_MODE = Integer.valueOf(Kad.prop.getProperty("kademlia.mode"));
            Kad.k = Integer.valueOf(Kad.prop.getProperty("kademlia.k"));
            Kad.alpha = Integer.valueOf(Kad.prop.getProperty("kademlia.alpha"));
            Kad.beta = Integer.valueOf(Kad.prop.getProperty("kademlia.beta"));
            Kad.isPrivate = Boolean.valueOf(Kad.prop.getProperty("ipfs.isprivate"));
            Kad.swarmKey = String.valueOf(Kad.prop.getProperty("ipfs.swarmkey"));
            Kad.providerPath = String.valueOf(Kad.prop.getProperty("ipfs.providerspath"));
            Kad.putRedundancy = Integer.valueOf(Kad.prop.getProperty("kademlia.putredundancy"));
            Kad.getdataPath = String.valueOf(Kad.prop.getProperty("ipfs.datapath"));
            Kad.GW_ENDPOINT = String.valueOf(Kad.prop.getProperty("ipfs.endpoint"));


            File f = new File("peerlist");
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = br.readLine();
            while (line != null) {
                Kad.peerList.add(line);
                line = br.readLine();
            }
            br.close();

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * Configをダウンロードします．
     */
    public void downloadConfig(String fileName){
        try {
            URL url = new URL("http://localhost/image1.jpg");
            HttpURLConnection conn =
                    (HttpURLConnection) url.openConnection();
            conn.setAllowUserInteraction(false);
            conn.setInstanceFollowRedirects(true);
            conn.setRequestMethod("GET");
            conn.connect();

            int httpStatusCode = conn.getResponseCode();
            if (httpStatusCode != HttpURLConnection.HTTP_OK) {
                throw new Exception("HTTP Status " + httpStatusCode);
            }

            String contentType = conn.getContentType();
            System.out.println("Content-Type: " + contentType);

            // Input Stream
            DataInputStream dataInStream
                    = new DataInputStream(
                    conn.getInputStream());

            // Output Stream
            DataOutputStream dataOutStream
                    = new DataOutputStream(
                    new BufferedOutputStream(
                            new FileOutputStream("/Users/keisukeo/image1.jpg")));

            // Read Data
            byte[] b = new byte[4096];
            int readByte = 0;

            while (-1 != (readByte = dataInStream.read(b))) {
                dataOutStream.write(b, 0, readByte);
            }

            // Close Stream
            dataInStream.close();
            dataOutStream.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }

    }

    public DatabaseRecordStore getStore() {
        return store;
    }

    public void setStore(DatabaseRecordStore store) {
        this.store = store;
    }

    public static String genAttrMask(String key, int i) {

        String strVal = null;
        return Kad.genAttrMask(key, String.valueOf(i));
    }

    public static String genNormalizedValue(int i){
        //int i = Integer.parseInt(value);
        String strVal = null;
        if(i >= 0 && i < 10){
            strVal = "0" + String.valueOf(i);
        }else{
            strVal = String.valueOf(i);
        }

        return strVal;
    }

    public static boolean isOwnHost(PeerAddresses addr){
        Host us = Kad.getIns().node;
        Multihash node1Id = Multihash.deserialize(us.getPeerId().getBytes());

        if(addr.peerId.toString().equals(node1Id.toString())){
            return true;

        }else{
            return false;

        }
    }


    /**
     * 現状はInt型のみ対応
     * @param key
     * @param value
     * @return
     */
    public static String genAttrMask(String key, String value){
        int i = Integer.parseInt(value);
        String strVal = null;
        if(i >= 0 && i < 10){
            strVal = "0" + String.valueOf(i);
        }else{
            strVal = String.valueOf(i);
        }
        StringBuffer buf = new StringBuffer(key);
        buf.append("^");
        buf.append(strVal);

        return buf.toString();
    }
    /**
     * Mapから，MerkleDAGにあるRawデータを取得する．
     * @param map
     * @return
     */
    public static byte[] getDataFromMerkleDAG(CborObject.CborMap map){
        byte[] rawData = map.getByteArray("RawData");
        return rawData;
    }

    /**
     * cidのみで，必要なRawデータを取得する．
     * @param cid
     * @return
     */
    public static byte[] getDataFromMerkleDAG(String cid){
        //MerkleDAGを読み込む．
        CborObject.CborMap map = Kad.readMerkleDAG(cid);
        byte[] data = Kad.getDataFromMerkleDAG(map);

        return data;
    }

    public static Object toObject(byte[] bytes) throws OptionalDataException, StreamCorruptedException, ClassNotFoundException, IOException{
        return new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
    }

    public static PeerAddresses genPeerAddresses(String str){
        LinkedList<MultiAddress> mList = new LinkedList<MultiAddress>();
        String multihash = null;
        if(str.indexOf(":") == -1){
            multihash = str;
            String mh = str.trim();
            MultiAddress addr = new MultiAddress("/ip4/"+Kad.getIns().getGlobalIP()+"/tcp/4001");
            mList.add(addr);

        }else{
            int idx = str.indexOf(":");
            multihash = str.substring(0, str.indexOf(":"));
            String addrlist_tmp = str.substring(idx+1);
            String raw_addrlist = addrlist_tmp.substring(2, addrlist_tmp.length()-1);
            StringTokenizer token = new StringTokenizer(raw_addrlist, ",");
            while(token.hasMoreTokens()){
                String v = token.nextToken();
                v = v.trim();
                MultiAddress addr = new MultiAddress(v);

                mList.add(addr);

            }
        }






        return new PeerAddresses(Multihash.fromBase58(multihash), mList);
    }

    public static boolean writeMerkleDAG(String cid, CborObject.CborMap map){
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            File dir = new File(p2 + "/" + Kad.providerPath);
            if(!dir.exists()){
               dir.mkdir();
            }
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(p2 + "/"+Kad.providerPath + "/"+cid));
            oos.writeObject(map);
            oos.close();
        }catch(Exception e){
            e.printStackTrace();
        }




        return true;
    }

    public static boolean isFileExists(String cid){
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            String path = p2 + "/"+Kad.providerPath + "/"+cid;
            Path p = Paths.get(path);

            if(Files.exists(p)){
                return true;
            }else{
                return false;
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        return false;
    }

    public static CborObject.CborMap readMerkleDAG(String cid){
        CborObject.CborMap obj = null;
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(p2 + "/"+Kad.providerPath + "/"+cid));
            obj = (CborObject.CborMap)ois.readObject();
        }catch(Exception e){
            e.printStackTrace();
        }
        return obj;

    }

    public void setGlobalIP(String globalIP) {
        this.globalIP = globalIP;
    }


    public Kademlia getKadDHT() {
        return kadDHT;
    }

    public void setKadDHT(Kademlia kadDHT) {
        this.kadDHT = kadDHT;
    }

    public String getGlobalIP(){
        BufferedReader in = null;
        String ip = null;
        if(Kad.getIns().globalIP != null){
            return Kad.getIns().globalIP;
        }
        try {
            URL whatismyip = new URL("http://checkip.amazonaws.com");

            in = new BufferedReader(new InputStreamReader(
                    whatismyip.openStream()));
            ip = in.readLine();
            Kad.getIns().setGlobalIP(ip);
            return ip;
        } catch(Exception e) {
            e.printStackTrace();

        }finally   {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return ip;
    }

    public Cid getCid(Multihash hash){
        Cid cid = Cid.build(1, Cid.Codec.Raw, hash);
        //Cid cid = new Cid(0, Cid.Codec.Raw, Multihash.Type.id, Hash.sha256(content.getBytes()));

        return cid;
    }

    public static Cid genCid(String content){
        Cid cid = new Cid(0, Cid.Codec.Raw, Multihash.Type.id, Hash.sha256(content.getBytes()));
        byte[] bbytes;
       /* try{
            bbytes = new Base32().decode(cid.toString());


        }catch(Exception e){

        }
        byte[] cBytes = content.getBytes();*/

        return cid;
    }

    public ProviderStore getProviderStore() {
        return providerStore;
    }

    public void setProviderStore(ProviderStore providerStore) {
        this.providerStore = providerStore;
    }

    public Host getNode() {
        return node;
    }

    public void setNode(Host node) {
        this.node = node;
    }

    public PeerAddresses getOwnAddresses() {
        return ownAddresses;
    }

    public void setOwnAddresses(PeerAddresses ownAddresses) {
        this.ownAddresses = ownAddresses;
    }

    public static Kad getIns(){
        if(Kad.own == null){
            Kad.own = new Kad();
        }
        return Kad.own;
    }
    public boolean isPeerExist(Multiaddr addr){

        if(!Kad.isPrivate){
           return true;
        }
        //IP4アドレスの取得
        String multiAddr = addr.toString();
        boolean ret = false;
        String regex = "((([01]?\\d{1,2})|(2[0-4]\\d)|(25[0-5]))\\.){3}(([01]?\\d{1,2})|(2[0-4]\\d)|(25[0-5]))";
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(multiAddr);
        if (m.find()) {
            String ip = m.group();

            Iterator<String> pIte = Kad.peerList.iterator();
            while(pIte.hasNext()){
                String orgIp = pIte.next();
                if(orgIp.equals(ip)){
                    ret = true;
                    break;
                }
            }
            return ret;

        }else{
            return false;
        }
    }


    public int getAlpha(int bucketIndex){
        switch(Kad.KAD_MODE){
            case 0:
                return Kad.alpha;

            case 1:
                return Kad.alpha;

            case 2:
                return Kad.alpha;

            default:
                return Kad.alpha;

        }
    }

    public int getPutRedundancy(){
        switch(Kad.KAD_MODE){
            case 0:
                return Kad.putRedundancy;

            case 1:
                return Kad.putRedundancy;

            case 2:
                return Kad.putRedundancy;

            default:
                return Kad.putRedundancy;

        }
    }

}
