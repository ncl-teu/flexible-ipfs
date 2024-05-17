package org.ncl.kadrtt.core;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.apache.commons.math.random.RandomDataImpl;
import org.peergos.APIServer;
import org.peergos.Hash;
import org.peergos.PeerAddresses;
import org.peergos.cbor.CborObject;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.dht.pb.Dht;
import org.peergos.protocol.dnsaddr.DnsAddr;


import java.io.*;
import java.math.BigDecimal;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Kad {

    private static Kad own;

    private static Properties prop;

    public static int KAD_MODE;

    public static int k;

    public static int alpha;


    public static int beta;

    public static boolean isPrivate;

    //public static String swarmKey;

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

    private HashMap<String, ContentInfo> cMap;

    private HttpServer server;

    private static final String OS_NAME = System.getProperty("os.name").toLowerCase();

    public static String CP;

    public static double lambda;

    public static double lambda_min;

    public static double lambda_max;

    public static RandomDataImpl rDataGen =  new RandomDataImpl();

    public static boolean leave;

    public static boolean replication;

    public static long replication_duration;

    public static double leave_min;

    public static double leave_max;

    public static long putstart_min;

    public static long putstart_max;

    public static long putinterval_min;

    public static long putinterval_max;

    public static long putcontent_min;

    public static long putcontent_max;

    public static long chunk_size;

    public static int chunkputthreads;

    public static int chunkgetthreads;

    public static ExecutorService chunkExec;

    public static ExecutorService chunkGetExec;

    public static HashMap<String, String> cidFileMap;


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
            //Kad.swarmKey = String.valueOf(Kad.prop.getProperty("ipfs.swarmkey"));
            Kad.providerPath = String.valueOf(Kad.prop.getProperty("ipfs.providerspath"));
            Kad.putRedundancy = Integer.valueOf(Kad.prop.getProperty("kademlia.putredundancy"));
            Kad.getdataPath = String.valueOf(Kad.prop.getProperty("ipfs.datapath"));
            Kad.GW_ENDPOINT = String.valueOf(Kad.prop.getProperty("ipfs.endpoint"));
            Kad.CP = String.valueOf(Kad.prop.get("ipfs.cp"));
            if(Kad.isLinux()){
                Kad.CP = Kad.CP.replaceAll(";", ":");
            }
            if(Kad.isWindows()){
                Kad.CP = Kad.CP.replaceAll(":", ";");
            }

            Kad.lambda_min = Double.valueOf(Kad.prop.getProperty("ipfs.lambda.min"));
            Kad.lambda_max = Double.valueOf(Kad.prop.getProperty("ipfs.lambda.max"));

            int leave_val = Integer.valueOf(Kad.prop.getProperty("ipfs.leave"));
            if(leave_val == 1){
                Kad.leave = true;
            }else{
                Kad.leave = false;
            }

            int replication_val = Integer.valueOf(Kad.prop.getProperty("ipfs.replication"));
            if(replication_val == 1){
                Kad.replication = true;
            }else{
                Kad.replication = false;
            }

            Kad.replication_duration = Long.valueOf(Kad.prop.getProperty("ipfs.replication.measure.duration"));

            Kad.leave_min = Double.valueOf(Kad.prop.getProperty("ipfs.lambda.leave.min")).doubleValue();
            Kad.leave_max = Double.valueOf(Kad.prop.getProperty("ipfs.lambda.leave.max")).doubleValue();

            Kad.putstart_min = Long.valueOf(Kad.prop.getProperty("ipfs.put.starttime_min")).longValue();
            Kad.putstart_max = Long.valueOf(Kad.prop.getProperty("ipfs.put.starttime_max")).longValue();

            Kad.putinterval_min = Long.valueOf(Kad.prop.getProperty("ipfs.put.interval_min")).longValue();
            Kad.putinterval_max = Long.valueOf(Kad.prop.getProperty("ipfs_put.interval_max")).longValue();

            Kad.putcontent_min = Long.valueOf(Kad.prop.getProperty("ipfs.put.contentnum_min")).longValue();
            Kad.putcontent_max = Long.valueOf(Kad.prop.getProperty("ipfs.put.contentnum_max")).longValue();
            Kad.chunk_size = 1024* Long.valueOf(Kad.prop.getProperty("ipfs.chunk.size")).longValue();

            Kad.chunkputthreads = Integer.valueOf(Kad.prop.getProperty("ipfs.chunkputthreads")).intValue();

            Kad.chunkgetthreads = Integer.valueOf(Kad.prop.getProperty("ipfs.chunkgetthreads")).intValue();

            Kad.cidFileMap = new HashMap<String, String>();

            this.cMap = new HashMap<String, ContentInfo>();
            this.configContentMap();
            //スレッドプールを作成する．
            Kad.chunkExec = Executors.newFixedThreadPool(Kad.chunkputthreads);
            Kad.chunkGetExec = Executors.newFixedThreadPool(Kad.chunkgetthreads);

            //Dataフォルダを見て，各要素をcMapへ格納する．

/*

            File f = new File("peerlist");
            BufferedReader br = new BufferedReader(new FileReader(f));
            String line = br.readLine();
            while (line != null) {
                Kad.peerList.add(line);
                line = br.readLine();
            }
            br.close();

*/
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public String getEndPointPeerId() {

        int idx = Kad.GW_ENDPOINT.indexOf("ipfs/");
        String gid = Kad.GW_ENDPOINT.substring(idx + 5);
        return gid;
    }

    public static HashMap<String, String> getCidFileMap() {
        return cidFileMap;
    }

    public static void setCidFileMap(HashMap<String, String> cidFileMap) {
        Kad.cidFileMap = cidFileMap;
    }

    public HttpServer getServer() {
        return server;
    }

    public void setServer(HttpServer server) {
        this.server = server;
    }



    private void configContentMap(){
        File dir = new File(Kad.getdataPath);
        File[] files = dir.listFiles();
        int len  = files.length;
        for(int i=0;i<len;i++){
            this.registerContentInfo(files[i].getName());
        }
    }


    public static boolean isLinux(){
        return OS_NAME.startsWith("linux");
    }

    public static boolean isWindows(){
        return OS_NAME.startsWith("windows");
    }

    public static boolean isMac(){
        return OS_NAME.startsWith("mac");
    }


    public void isBootStrap(){


    }

    public void removeContentInfo(String cid){
        this.cMap.remove(cid);
    }
    /**
     * 指定CIDのアクセスカウントを上げる．
     * @param cid
     */
    public void addAccessCount(String cid){
        if(this.cMap.containsKey(cid)){
            ContentInfo info = this.cMap.get(cid);
            long currentVal = info.getCount();
            currentVal++;
            info.setCount(currentVal);
        }else{
            //なければ，新規作成
            this.registerContentInfo(cid);
            this.addAccessCount(cid);

        }
    }

    public static LinkedList<Cid> genCidList(LinkedList<byte[]> chunkList){
        Iterator<byte[]> bIte = chunkList.iterator();
        LinkedList<Cid> cidList = new LinkedList<Cid>();
        //cidリストを作成する．
        while(bIte.hasNext()){
            byte[] ch = bIte.next();
            Cid ccid = Kad.genCid(ch);
            cidList.add(ccid);
        }

        return cidList;
    }

    public static LinkedList<byte[]> genChunkList(byte[] allByte){
        LinkedList<byte[]> chunkList = new LinkedList<byte[]>();

        long len = allByte.length;
        long j = 1;
        long size = len / Kad.chunk_size;
        if(len % Kad.chunk_size == 0){
        }else{
            size++;
        }

        for(long i=0;i<size;i++){
            byte[] chunks;
            if(i == size - 1){
                chunks = Arrays.copyOfRange(allByte, (int)(Kad.chunk_size * i), (int)(len-1));
            }else{
                chunks = Arrays.copyOfRange(allByte, (int)(Kad.chunk_size * i), (int)(Kad.chunk_size * (i+1)));
            }
            chunkList.add(chunks);
        }
        return chunkList;
    }

    public void registerContentInfo(String cid){
        ContentInfo info = new ContentInfo(cid);
        this.cMap.put(cid, info);
    }

   /* public String getSwarmKey(){
        return Kad.swarmKey;
    }

    */

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


    public static PeerAddresses genForeignPeerAddresses(String str){
        LinkedList<MultiAddress> mList = new LinkedList<MultiAddress>();
        String multihash = null;
        if(str.indexOf(":") == -1){
            multihash = str;
            String mh = str.trim();
            MultiAddress addr = new MultiAddress(str);
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

    public static int getBeta() {
        return beta;
    }

    public static void setBeta(int beta) {
        Kad.beta = beta;
    }

    /**
     * cidとmapをキーにして，そこから指定の場所にファイルを書き出します．
     * @param cid
     * @param map
     * @return
     */
    public static void writeData(String cid, CborObject.CborMap map){
        try{
            //byte[] data = map.getByteArray("RawData");
            CborObject.CborByteArray data = (CborObject.CborByteArray) map.get("RawData");

            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();

            File dir_data = new File(p2 + File.separator + Kad.getdataPath);
            if(!dir_data.exists()){
                dir_data.mkdir();
            }

            ObjectOutputStream oosdata = new ObjectOutputStream(new FileOutputStream(p2 + File.separator+Kad.getdataPath + File.separator+cid));
            oosdata.writeObject(data);
            oosdata.close();


        }catch(Exception e){
            e.printStackTrace();
        }

    }

    /**
     * MerkleDAGとコンテンツを保存する処理です．
     * MerkleDAG: providersフォルダ（コンテンツそのものは保存しない）
     * コンテンツ: getdataフォルダ
     * に保存されます．
     * その後は，cMapに登録します．
     * @param cid
     * @param map
     * @return
     */
    public static boolean writeMerkleDAG(String cid, CborObject.CborMap map){
        try{

            //データ部の書き込み
            Kad.writeData(cid, map);

            CborObject cbor = CborObject.fromByteArray(map.toByteArray());
            //if (! (cbor instanceof CborObject.CborMap))
            CborObject.CborMap map2 = (CborObject.CborMap) cbor;
            map2.remove(new CborObject.CborString("RawData"));


            //書き出しができたら，MerkleDAGからRawDataを削除する．
           // map.put("RawData", null);
            //map.remove("RawData");
           // map.remove(new CborObject.CborString("RawData"));

            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();

            File dir = new File(p2 + File.separator + Kad.providerPath);
            if(!dir.exists()){
               dir.mkdir();
            }

            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(p2 + File.separator+Kad.providerPath + File.separator+cid));
            oos.writeObject(map2);
            oos.close();

            //cMapへ登録する．
            Kad.getIns().addAccessCount(cid);



        }catch(Exception e){
            e.printStackTrace();
        }




        return true;
    }

    public static boolean writeMerkleDAGOnly(String cid, CborObject.CborMap map){
        try{


            CborObject cbor = CborObject.fromByteArray(map.toByteArray());
            //if (! (cbor instanceof CborObject.CborMap))
            CborObject.CborMap map2 = (CborObject.CborMap) cbor;


            //書き出しができたら，MerkleDAGからRawDataを削除する．
            // map.put("RawData", null);
            //map.remove("RawData");
            // map.remove(new CborObject.CborString("RawData"));

            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();

            File dir = new File(p2 + File.separator + Kad.providerPath);
            if(!dir.exists()){
                dir.mkdir();
            }

            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(p2 + File.separator+Kad.providerPath + File.separator+cid));
            oos.writeObject(map2);
            oos.close();

            //cMapへ登録する．
            Kad.getIns().addAccessCount(cid);



        }catch(Exception e){
            e.printStackTrace();
        }




        return true;
    }

    public static boolean isFileExists(String cid){
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            String path = p2 + File.separator+Kad.getdataPath + File.separator+cid;
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
    public static CborObject.CborMap readMerkleDAGOnly(String cid){
        CborObject.CborMap obj = null;
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            Path p = Paths.get(p2 + File.separator+Kad.providerPath + File.separator+cid);
            if(!Files.exists(p)){
                return null;
            }
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(p2 + File.separator+Kad.providerPath + File.separator+cid));
            obj = (CborObject.CborMap)ois.readObject();
            ois.close();

            //最後に，cMapのカウントアップする．
            if(Kad.getIns().cMap.containsKey(cid)){
                Kad.getIns().addAccessCount(cid);

            }else{
                Kad.getIns().registerContentInfo(cid);
                Kad.getIns().addAccessCount(cid);
            }
            return obj;

        }catch(Exception e){
            e.printStackTrace();
        }
        return obj;

    }

    /**
     * チャンクのみを取得します．
     * @param cid
     * @return
     */
    public static CborObject.CborByteArray readChunk(String cid){
        CborObject.CborByteArray obj = null;
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            Path p = Paths.get(p2 + File.separator+Kad.getdataPath + File.separator+cid);
            if(!Files.exists(p)){
                return null;
            }
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(p2 + File.separator+Kad.getdataPath + File.separator+cid));

            CborObject.CborByteArray cdata = (CborObject.CborByteArray) ois.readObject();
            //obj = (CborObject.CborMap)ois.readObject();
            ois.close();

            //最後に，cMapのカウントアップする．
            if(Kad.getIns().cMap.containsKey(cid)){
                Kad.getIns().addAccessCount(cid);

            }else{
                Kad.getIns().registerContentInfo(cid);
                Kad.getIns().addAccessCount(cid);
            }
            return cdata;

        }catch(Exception e){
            e.printStackTrace();
        }
        return obj;

    }

    public static boolean isChunkExists(String cid){
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            Path p = Paths.get(p2 + File.separator+Kad.getdataPath + File.separator+cid);
            boolean isExists = false;
            if(Files.exists(p)){
                isExists = true;
            }

            return isExists;
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
            Path p = Paths.get(p2 + File.separator+Kad.providerPath + File.separator+cid);
            if(!Files.exists(p)){
                return null;
            }
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(p2 + File.separator+Kad.providerPath + File.separator+cid));
            obj = (CborObject.CborMap)ois.readObject();
            ois.close();

            //次に，dataを読み出す．
            //byte[]である．
            ObjectInputStream bis = new ObjectInputStream(new FileInputStream(p2 + File.separator + Kad.getdataPath + File.separator + cid));
            CborObject.CborByteArray cdata = (CborObject.CborByteArray) bis.readObject();
            bis.close();
          //  byte[] data = bis.readAllBytes();
        //    CborObject cObj = CborObject.fromByteArray(data);

          //  CborObject.CborByteArray cdata = (CborObject.CborByteArray) cObj;
            obj.put("RawData", cdata);
            //obj.put("cid", new CborObject.CborString(cid));


            //最後に，cMapのカウントアップする．
            if(Kad.getIns().cMap.containsKey(cid)){
                Kad.getIns().addAccessCount(cid);

            }else{
                Kad.getIns().registerContentInfo(cid);
                Kad.getIns().addAccessCount(cid);
            }
            return obj;

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

    public static LinkedList<InetAddress>  getAllIP() throws Exception {
        Enumeration <NetworkInterface> netSet;//集合内の列挙操作に使う古い型
        netSet = NetworkInterface.getNetworkInterfaces();
        LinkedList<InetAddress> addList = new LinkedList<InetAddress>();

        while(netSet.hasMoreElements()){//すべてのインターフェイスを走査
            NetworkInterface nInterface = (NetworkInterface) netSet.nextElement();
            List<InterfaceAddress>list = nInterface.getInterfaceAddresses();

            if( list.size() == 0 ) continue;
            //System.out.println(nInterface .getName() );//ネットワーク識別名
            for (InterfaceAddress interfaceAdr : list){
                InetAddress inet = interfaceAdr.getAddress();
                addList.add(inet);
                // IP.print(inet);//IPアドレスの表示
            }
        }
        return addList;
    }

    public static Cid genCid(byte[] content){
        Cid cid = new Cid(0, Cid.Codec.Raw, Multihash.Type.id, Hash.sha256(content));
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

    public boolean checkPeer(PeerId peer, Multiaddr addr){

        Host us = Kad.getIns().getNode();
        String str = addr.toString();
        String foreignStr = peer.toString() + ": ["+ addr.toString() + "]";
        PeerAddresses addrs = Kad.genForeignPeerAddresses(foreignStr);
        try{
            Dht.Record res = Kad.getIns().getKadDHT().dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().checkPrivate().join();
            System.out.println();
            return true;
        }catch(Exception e){
            return false;
        }
    }


    public static double genDouble(double min, double max){
        return Kad.getRoundedValue(min + (double) (Math.random() * (max - min + 1)));

    }

    public static long genLong(long min, long max) {

        return min + (long) (Math.random() * (max - min + 1));

    }

    public static int genInt(int  min, int max) {

        return min + (int) (Math.random() * (max - min + 1));

    }

    public static double getRoundedValue(double value1) {
        //  try{
        BigDecimal value2 = new BigDecimal(String.valueOf(value1));
        double retValue = value2.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
        return retValue;

    }


    /**
     * Int型の，一様／正規分布出力関数
     * @param min
     * @param max
     * @param dist
     * @param mu
     * @return
     */
    public static int genInt2(int min, int  max, int dist, double mu){
        max = Math.max(min, max);

        if(min == max){
            return min;
        }
        if(dist== 0){
            //一様分布
            return min + (int) (Math.random() * (max - min + 1));

        }else{
            //正規分布
            double meanValue2 = min + (max-min)* mu;
            double sig = (double)(Math.max((meanValue2-min), (max-meanValue2)))/3;
            double ran2 = Kad.rDataGen.nextGaussian(meanValue2,sig);


            if(ran2 < min){
                ran2 =(int) min;
            }

            if(ran2 > max){
                ran2 = (int)max;
            }

            return (int) ran2;
        }

    }

    /**
     * Double型の，一様／正規分布出力関数
     * @param min
     * @param max
     * @param mu
     * @return
     */
    public static double  genDouble2(double min, double max, double mu){
        if(min == max){
            return min;
        }

        //正規分布
        double meanValue2 = min + (max-min)* mu;
        double sig = (double)(Math.max((meanValue2-min), (max-meanValue2)))/3;
        double ran2 = Kad.getRoundedValue(Kad.rDataGen.nextGaussian(meanValue2,sig));


        if(ran2 < min){
            ran2 =(double) min;
        }

        if(ran2 > max){
            ran2 = (double)max;
        }

        return (double) ran2;


    }


    /**
     * Long型の一様・正規分布出力関数
     * @param min
     * @param max
     * @param dist
     * @param mu
     * @return
     */
    public static long genLong2(long min, long max, int dist, double mu){
        if(min == max){
            return min;
        }
        if(dist== 0){
            //一様分布
            return min + (long) (Math.random() * (max - min + 1));

        }else{
            //正規分布
            double meanValue2 = min + (max-min)* mu;
            double sig = (double)(Math.max((meanValue2-min), (max-meanValue2)))/3;
            double ran2 = Kad.rDataGen.nextGaussian(meanValue2,sig);


            if(ran2 < min){
                ran2 =(double) min;
            }

            if(ran2 > max){
                ran2 = (double)max;
            }

            return (long) ran2;
        }

    }


    public boolean isPeerExist(PeerId peer, Multiaddr addr){


        return true;
/*

        List<String> resolved = new LinkedList<String>();
        String rawStr = addr.toString() + "/ipfs/"+peer.toString();
        resolved.add(rawStr);
        List<? extends CompletableFuture<? extends KademliaController>> futures = resolved.stream()
                .parallel()
                //.map(taddr -> Kad.getIns().getKadDHT().dial(this.node, Multiaddr.fromString(taddr)).getController())
                .map(taddr -> Kad.getIns().getKadDHT().dial(this.node, Multiaddr.fromString(taddr)).getController())
                .collect(Collectors.toList());
        int successes = 0;
        for (CompletableFuture<? extends KademliaController> future : futures) {
            try {

                future.orTimeout(5, TimeUnit.SECONDS).join();
                successes++;
                Dht.Record res = future.get().checkPrivate().join();
                ByteString bs = res.getValue();
                CborObject cbor = CborObject.fromByteArray(bs.toByteArray());
                CborObject.CborString incommingSwarmKey= (CborObject.CborString)cbor;
                //swarm keyチェック
                if(Kad.getIns().getSwarmKey().equals(incommingSwarmKey)){
                    return true;
                }else{
                    return false;
                }


            } catch (Exception e) {
                return false;
            }

        }

*/
        /*
        Host us = Kad.getIns().getNode();
        String str = addr.toString();
        String foreignStr = peer.toString() + ": ["+ addr.toString() + "]";
        PeerAddresses addrs = Kad.genForeignPeerAddresses(foreignStr);
        try{
            Dht.Record res = Kad.getIns().getKadDHT().dialPeer(addrs, us).orTimeout(5, TimeUnit.SECONDS).join().checkPrivate().join();
            System.out.println();
            return true;
        }catch(Exception e){
            return false;
        }
*/


       // ByteString bs = res.getValue();


/*

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
*/

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
