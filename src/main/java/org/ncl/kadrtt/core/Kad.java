package org.ncl.kadrtt.core;

import com.google.protobuf.ByteString;
import com.sun.net.httpserver.HttpServer;
import io.ipfs.cid.Cid;
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.AddressBook;
import io.libp2p.core.Host;
import io.libp2p.core.PeerId;
import io.libp2p.core.multiformats.Multiaddr;
import org.apache.commons.math.random.RandomDataImpl;
import org.peergos.APIServer;
import org.peergos.Hash;
import org.peergos.PeerAddresses;
import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;
import org.peergos.protocol.dht.*;
import org.peergos.protocol.dht.pb.Dht;
import org.peergos.protocol.dnsaddr.DnsAddr;


import java.io.*;
import java.math.BigDecimal;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    public KademliaEngine kadEngine;

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

    public static String attr_file = "attr";

    public AddressBook book;

    public static String ABSOLUTE_DATAPATH;



    /**
     * 属性情報マップ．<time,2>といった，<属性名, 最小桁数>を表す．
     */
    public static HashMap<String, Integer> attrMap;


    /**
     * DL中のコンテンツのマップ
     * <CID, Set<chunkのCID>>
     */
    public static HashMap<String, PendingContent> pMap;


    private Kad(){


    }



    public void initialize(String fileName){
        try {
            Kad.attrMap = new HashMap<String, Integer>();

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
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();

            //String p3 = p2 + File.separator + Kad.getdataPath +
            Kad.ABSOLUTE_DATAPATH = p2 + File.separator + Kad.getdataPath;

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

            Kad.pMap = new HashMap<String, PendingContent>();

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
            this.configAttrFilter();
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

    /**
     * 属性ファイルからの属性情報の初期化．
     * なければデフォルト値を返す．
     * @return
     */
    public void configAttrFilter(){
        Path p1 = Paths.get("");
        Path p2 = p1.toAbsolutePath();
        int filterNum = 2;


        try {
            //File f = new File("test.txt");
            File attrFile = new File(p2 + File.separator + Kad.attr_file);
            BufferedReader br = new BufferedReader(new FileReader(attrFile));
            String line;
            while ((line = br.readLine()) != null) {
                // 処理
                StringTokenizer token = new StringTokenizer(line, ",");
                String name = token.nextToken();
                Kad.attrMap.put(name, Integer.valueOf(token.nextToken()));

            }
            br.close();


        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    public int getFilterNum(String key){
        if(Kad.attrMap.containsKey(key)){
            return Kad.attrMap.get(key).intValue();
        }else{
            //デフォルト値
            return 2;
        }
    }

    /**
     *
     * @param cid
     * @return
     */
    public static File getDirDataPath(String cid){

        Path p1 = Paths.get("");
        Path p2 = p1.toAbsolutePath();
        String abs = Kad.ABSOLUTE_DATAPATH + File.separator + cid;
        //String p3 = p2 + File.separator + Kad.getdataPath +
        File dir_data = new File(abs);
        if(!dir_data.exists()) {
            dir_data.mkdir();
        }
        return dir_data;
    }

    public void registerpMap(String cid, ArrayList<CborObject.CborString> cidList){
        Iterator<CborObject.CborString> cidIte = cidList.iterator();
        LinkedHashSet<String> set = new LinkedHashSet<String>();
        PendingContent c;
        if(Kad.pMap.containsKey(cid)){
           //なにもせず，そのまま
        }else{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();

            //String p3 = p2 + File.separator + Kad.getdataPath +
            File dir_data = new File(Kad.ABSOLUTE_DATAPATH);

            while(cidIte.hasNext()){
                CborObject.CborString ccid = cidIte.next();
                String ccid_str = ccid.value;
                //もしdataディレクトリになければsetへ追加する．
                String chunkPath = dir_data + File.separator + ccid_str;
                Path wPath = Paths.get(chunkPath);
                if(Files.exists(wPath)){

                }else{
                    set.add(ccid_str);

                }
            }

            //set = new LinkedHashSet<String>();
            c = new PendingContent(cid, set, cidList);
            Kad.pMap.put(cid, c);

        }

    }

    public static String calcNow(){
        LocalDateTime nowDate = LocalDateTime.now();
        System.out.println(nowDate); //2020-12-20T13:32:48.293

        // 表示形式を指定
        DateTimeFormatter dtf1 =
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String formatNowDate = dtf1.format(nowDate);
        return formatNowDate;
    }

    public static String getExFromFile(String fName){
        String ex = "";
        int idx = fName.lastIndexOf(".");
        if(idx != -1){
            ex = fName.substring(idx+1);
        }
        return ex;
    }

    public void ChunkFinishProcess(String contentCid, String chunkCid, String ex){

        //まず，コンテンツ情報を取得する．
        PendingContent p = Kad.pMap.get(contentCid);
        LinkedHashSet<String> set = p.getSet();
       // int remainedNum = set.size();
        boolean isPreEmpty = set.isEmpty();
        //未完了チャンク情報を削除する．
        set.remove(chunkCid);
        //もし空っぽであれば，コンテンツ結合を行う．
        if(set.isEmpty() ){
            //getdataディレクトリにある全cidリストを順に，バイナリとして取得する．
            //ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            try{
                Path p1 = Paths.get("");
                Path p2 = p1.toAbsolutePath();

                File dir_data = new File(Kad.ABSOLUTE_DATAPATH);
                String file;
                if(ex != null){
                    file = contentCid + "." + ex;
                }else{
                    file = contentCid;
                }

                String writePath = dir_data + File.separator + file;
                //もしすでにファイルがあれば無視する．
                Path wPath = Paths.get(writePath);
                if(Files.exists(wPath)){
                    System.out.println("File:"+file + " is already Exists. Do Notiong.");
                    return;
                }else{
                    FileOutputStream fos = new FileOutputStream(writePath);
                    BufferedOutputStream bos = new BufferedOutputStream(fos);
                    Iterator<CborObject.CborString> cIte = p.getCidList().iterator();
                    while(cIte.hasNext()){
                        CborObject.CborString cstr = cIte.next();
                        String tmpCid = cstr.value;
                        //contentCid/tmpCidのファイルを取得する
                        ObjectInputStream bis = new ObjectInputStream(new FileInputStream(Kad.ABSOLUTE_DATAPATH + File.separator + tmpCid));
                        CborObject.CborByteArray cdata = (CborObject.CborByteArray) bis.readObject();
                        //byte[] cdata = bis.rea
                        bis.close();
                        byte[] data = cdata.value;

                        bos.write(data);


                    }
                    System.out.println("----File:" + file + " is generated.-----");
                    //Kad.pMap.remove(contentCid);
                }



            }catch(FileNotFoundException e){

            }catch(Exception e){
                e.printStackTrace();
            }



        }else{

        }


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
        File dir = new File(Kad.ABSOLUTE_DATAPATH);
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

    public static String genTagInfo(String name, String value){
        StringBuffer buf = new StringBuffer(name);
        buf.append("^");
        buf.append(value);

        return buf.toString();
    }

    public static String genTagMask(String key, String value){
        int keta = Kad.getIns().getFilterNum(key);
        String mask = value.substring(0, keta);

        return Kad.genTagInfo(key, mask);
    }

    public static String genNormalizedValue(String key, int i){
        //int i = Integer.parseInt(value);

        String strVal = null;
        int keta = Kad.getIns().getFilterNum(key);
        StringBuffer buf1 = new StringBuffer("%0");
        buf1.append(keta);
        buf1.append("d");
        strVal = String.format(buf1.toString(), i);



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
     * 数値から，0でパディングされた文字列を返します．
     * @param val
     * @return
     */
    public static String getFilteredValue(String key, int val){
        String strVal = null;
        int keta = Kad.getIns().getFilterNum(key);
        StringBuffer buf1 = new StringBuffer("%0");
        buf1.append(keta);
        buf1.append("d");
        strVal = String.format(buf1.toString(), val);
        return strVal;
    }

    /**
     * 現状はInt型のみ対応
     * 属性名^フィルタ値を返す．timeが0427で，桁数が2である場合，time^04を返す．
     * @param key
     * @param value
     * @return
     */
    public static String genAttrMask(String key, String value){
        //int dNum = Integer.valueOf(value).intValue();


        int dNum = Integer.parseInt(value);


        String strVal = String.valueOf(value).toString();

        int keta = Kad.getIns().getFilterNum(key);
        if(strVal.length() < keta){
            StringBuffer buf = new StringBuffer("0");
            buf.append(strVal);
            value = buf.toString();
        }
        String valMask = value.substring(0,keta);
        /*StringBuffer buf1 = new StringBuffer("%0");
        buf1.append(keta);
        buf1.append("d");

        strVal = String.format(buf1.toString(), dNum);

         */
        StringBuffer buf = new StringBuffer(key);
        buf.append("^");
        buf.append(valMask);

        return buf.toString();
    }

    /**
     * 属性名^生の値　を生成するメソッド．timeが0427であれば，time^0427を返す
     *
     * @param key
     * @param value
     * @return
     */
    public static String genAttrInfo(String key, String value){
        StringBuffer buf = new StringBuffer(key);
        buf.append("^");
        buf.append(value);

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

            File dir_data = new File(Kad.ABSOLUTE_DATAPATH);
            if(!dir_data.exists()){
                dir_data.mkdir();
            }

            ObjectOutputStream oosdata = new ObjectOutputStream(new FileOutputStream(Kad.ABSOLUTE_DATAPATH + File.separator+cid));
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

    public static boolean isDAGExists(String cid){
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            String path = p2 + File.separator+Kad.providerPath + File.separator+cid;
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

    public static boolean isFileExists(String cid){
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            String path = Kad.ABSOLUTE_DATAPATH + File.separator+cid;
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
            Path p = Paths.get(Kad.ABSOLUTE_DATAPATH + File.separator+cid);
            if(!Files.exists(p)){
                return null;
            }
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(Kad.ABSOLUTE_DATAPATH + File.separator+cid));

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
            Path p = Paths.get(Kad.ABSOLUTE_DATAPATH + File.separator+cid);
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

    /**
     * 属性名と属性値を含むコンテンツCIDのリストを返します．
     * @param attrMask
     * @return　コンテンツCIDのリスト
     */
    public static LinkedList<Cborable>  getContentCidFromAttr(String attrMask){
       // CborObject.CborMap obj = null;
        //attrMask.get
        int start = attrMask.indexOf("^");
        String org_key = attrMask.substring(0, start);

        String org_value = attrMask.substring(start+1);
        //DAG内のvalueが，このvalueから始まればOK．
        LinkedList<String> cidList = new LinkedList<String>();
        LinkedList<Cborable> retList = new LinkedList<Cborable>();

        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
           // Path p = Paths.get(p2 + File.separator+Kad.providerPath + File.separator);
            String path = p2 + File.separator+Kad.providerPath + File.separator;
            File dir = new File(path);
            File[] dags = dir.listFiles();
            int len = dags.length;
            for(int i=0;i<len;i++){
                File dag = dags[i];
                System.out.println("FileName:"+dag.getName());
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path + dag.getName()));
                CborObject.CborMap obj = (CborObject.CborMap)ois.readObject();
                ois.close();
                Iterator<String> keyIte = obj.keySet().iterator();
                while(keyIte.hasNext()){
                    String key = keyIte.next();
                    if(key.equals(org_key)){
                        CborObject.CborString val = (CborObject.CborString)obj.get(key);
                        //orgValueを先頭から含んでいればOK．
                        String str_val = val.value;
                        if(str_val.indexOf(org_value)==0){
                            obj.put("cid", new CborObject.CborString(dag.getName()));
                            retList.add(obj);
                           // cidList.add(dag.getName());
                            break;
                        }
                    }
                    //System.out.println("Key:"+ key + "/Value: " + obj.get(key));
                }


            }
            return retList;


        }catch(Exception e){
            e.printStackTrace();
        }
        return null;

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
            ObjectInputStream bis = new ObjectInputStream(new FileInputStream(Kad.ABSOLUTE_DATAPATH + File.separator + cid));
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

    public static void setPutRedundancy(int putRedundancy) {
        Kad.putRedundancy = putRedundancy;
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
