package org.peergos.net;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import io.ipfs.cid.Cid;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import io.libp2p.core.PeerId;
import org.ncl.kadrtt.core.Kad;
import org.peergos.*;
import org.peergos.blockstore.RamBlockstore;
import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;
import org.peergos.protocol.dht.RamProviderStore;
import org.peergos.protocol.dht.RamRecordStore;
import org.peergos.protocol.ipns.IPNS;
import org.peergos.protocol.ipns.pb.Ipns;
import org.peergos.util.HttpUtil;
import org.peergos.util.JSONParser;
import org.peergos.util.Logging;
import org.peergos.util.Version;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class APIHandler extends Handler {
    public static final String API_URL = "/api/v0/";
    public static final Version CURRENT_VERSION = Version.parse("0.0.1");
    private static final Logger LOG = Logging.LOG();

    private static final boolean LOGGING = true;

    public static final String ID = "id";
    public static final String VERSION = "version";
    public static final String GET = "block/get";
    public static final String PUT = "block/put";
    public static final String RM = "block/rm";
    public static final String STAT = "block/stat";
    public static final String REFS_LOCAL = "refs/local";
    public static final String BLOOM_ADD = "bloom/add";
    public static final String HAS = "block/has";

    public static final String FIND_PROVS = "dht/findprovs";

    //Added by Kanemitsu
    public static final String PUT_ATTR = "dht/putattr";

    public static final String PUT_ATTRWITHRANGE = "dht/putattrs";

    public static final String PUT_VALUE_ATTR = "dht/putvaluewithattr";

    public static final String GET_VALUE = "dht/getvalue";

    public static final String FIND_ATTRS = "dht/getbyattrs";

    public static final String DROP_ATTR = "dht/dropattr";

    private final EmbeddedIpfs ipfs;

    public APIHandler(EmbeddedIpfs ipfs) {
        this.ipfs = ipfs;
    }

    public void handleCallToAPI(HttpExchange httpExchange) {

        long t1 = System.currentTimeMillis();
        String path = httpExchange.getRequestURI().getPath();
        try {
            if (!path.startsWith(API_URL))
                throw new IllegalStateException("Unsupported api version, required: " + API_URL);
            path = path.substring(API_URL.length());
            // N.B. URI.getQuery() decodes the query string
            Map<String, List<String>> params = HttpUtil.parseQuery(httpExchange.getRequestURI().getQuery());
            List<String> args = params.get("arg");

            switch (path) {
                case ID: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-id
                    PeerId peerId = ipfs.node.getPeerId();
                    Map res = new HashMap<>();
                    res.put("ID", peerId.toBase58());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case VERSION: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-version
                    Map res = new HashMap<>();
                    res.put("Version", CURRENT_VERSION.toString());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case GET: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-get
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"ipfs-path\" is required");
                    }
                    Optional<String> auth = Optional.ofNullable(params.get("auth")).map(a -> a.get(0));
                    Set<PeerId> peers = Optional.ofNullable(params.get("peers"))
                            .map(p -> p.stream().map(PeerId::fromBase58).collect(Collectors.toSet()))
                            .orElse(Collections.emptySet());
                    boolean addToBlockstore = Optional.ofNullable(params.get("persist"))
                            .map(a -> Boolean.parseBoolean(a.get(0)))
                            .orElse(true);
                    List<HashedBlock> block = ipfs.getBlocks(List.of(new Want(Cid.decode(args.get(0)), auth)), peers, addToBlockstore);
                    if (!block.isEmpty()) {
                        replyBytes(httpExchange, block.get(0).block);
                    } else {
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }
                    break;
                }
                case PUT: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-put
                    List<String> format = params.get("format");
                    Optional<String> formatOpt = format != null && format.size() == 1 ? Optional.of(format.get(0)) : Optional.empty();
                    if (formatOpt.isEmpty()) {
                        throw new APIException("argument \"format\" is required");
                    }
                    String reqFormat = formatOpt.get().toLowerCase();
                    String boundary = httpExchange.getRequestHeaders().get("Content-Type")
                            .stream()
                            .filter(s -> s.contains("boundary="))
                            .map(s -> s.substring(s.indexOf("=") + 1))
                            .findAny()
                            .get();
                    List<byte[]> data = MultipartReceiver.extractFiles(httpExchange.getRequestBody(), boundary);
                    if (data.size() != 1) {
                        throw new APIException("Multiple input not supported");
                    }
                    byte[] block = data.get(0);
                    if (block.length > 1024 * 1024 * 2) { //todo what should the limit be?
                        throw new APIException("Block too large");
                    }
                    Cid cid = ipfs.blockstore.put(block, Cid.Codec.lookupIPLDName(reqFormat)).join();
                    Map res = new HashMap<>();
                    res.put("Hash", cid.toString());
                    replyJson(httpExchange, JSONParser.toString(res));
                    break;
                }
                case RM: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-rm
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Cid cid = Cid.decode(args.get(0));
                    boolean deleted = ipfs.blockstore.rm(cid).join();
                    if (deleted) {
                        Map res = new HashMap<>();
                        res.put("Error", "");
                        res.put("Hash", cid.toString());
                        replyJson(httpExchange, JSONParser.toString(res));
                    } else {
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }
                    break;
                }
                case STAT: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-block-stat
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Optional<String> auth = Optional.ofNullable(params.get("auth")).map(a -> a.get(0));
                    List<HashedBlock> block = ipfs.getBlocks(List.of(new Want(Cid.decode(args.get(0)), auth)), Collections.emptySet(), false);
                    if (!block.isEmpty()) {
                        Map res = new HashMap<>();
                        res.put("Size", block.get(0).block.length);
                        replyJson(httpExchange, JSONParser.toString(res));
                    } else {
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }
                    break;
                }
                case REFS_LOCAL: { // https://docs.ipfs.tech/reference/kubo/rpc/#api-v0-refs-local
                    List<Cid> refs = ipfs.blockstore.refs().join();
                    StringBuilder sb = new StringBuilder();
                    for (Cid cid : refs) {
                        Map<String, String> entry = new HashMap<>();
                        entry.put("Ref", cid.toString());
                        entry.put("Err", "");
                        sb.append(JSONParser.toString(entry));
                    }
                    replyBytes(httpExchange, sb.toString().getBytes());
                    break;
                }
                case HAS: {
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"ipfs-path\" is required");
                    }
                    boolean has = ipfs.blockstore.has(Cid.decode(args.get(0))).join();
                    replyBytes(httpExchange, has ? "true".getBytes() : "false".getBytes());
                    break;
                }
                case BLOOM_ADD: {
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Boolean added = ipfs.blockstore.bloomAdd(Cid.decode(args.get(0))).join();
                    replyBytes(httpExchange, added.toString().getBytes());
                    break;
                }
                case FIND_PROVS: {
                    if (args == null || args.size() != 1) {
                        throw new APIException("argument \"cid\" is required\n");
                    }
                    Optional<Integer> providersParam = Optional.ofNullable(params.get("num-providers")).map(a -> Integer.parseInt(a.get(0)));
                    int numProviders = providersParam.isPresent() && providersParam.get() > 0 ? providersParam.get() : 20;
                    List<PeerAddresses> providers = ipfs.dht.findProviders(Cid.decode(args.get(0)), ipfs.node, numProviders).join();

                    StringBuilder sb = new StringBuilder();
                    Map<String, Object> entry = new HashMap<>();
                    Map<String, Object> responses = new HashMap<>();
                    for (PeerAddresses provider : providers) {
                        List<String> addresses = provider.addresses.stream().map(a -> a.toString()).collect(Collectors.toList());
                        responses.put("Addrs", addresses);
                        responses.put("ID", provider.peerId.toBase58());
                    }
                    entry.put("Responses", responses);
                    sb.append(JSONParser.toString(entry) + "\n");
                    replyBytes(httpExchange, sb.toString().getBytes());
                    break;
                }
                //属性つきコンテンツのPUT
                //コンテンツ + 属性情報をPUTするが，ここで，CborMap内に値をセットして，putする感じ．
                //属性情報がなければ通常のPUT処理となる．属性情報があれば，属性プロセスへと移る．
                //その後，指定の属性情報の担当ノードを見つけて(findNode)，それに対してDB登録リクエストを行う
                case PUT_VALUE_ATTR:
                    //attr=属性名1:値1^属性名2:値2^....
                    //curl -X POST "http://127.0.0.1:5001/api/v0/dht/putvaluewithattr?value=xxxx&attr=time_0834-location_doka"


                    ///dht/putvaluewithattr?file=xxxx&attrname=time&attrvalue=0825
                    //ファイルをputする．
                    //また，属性値も付与する．
                    Optional<String> file = Optional.ofNullable(params.get("file")).map(a -> a.get(0));
                    Optional<String> in_val = Optional.ofNullable(params.get("value")).map(a -> a.get(0));
                    Optional<String> in_attrs = Optional.ofNullable(params.get("attr")).map(a -> a.get(0));
                    // Optional<String> attrValueOpt = Optional.ofNullable(params.get("attrvalue")).map(a -> a.get(0));

                    String content = null;
                    if (file.isEmpty()) {
                        //throw new APIException("argument \"cid\" is required\n");
                        if (in_val.isEmpty()) {
                            throw new APIException("file or value should be specified.");
                        } else {
                            content = in_val.get();
                        }

                    } else {
                        Path filePath = Paths.get(file.get());
                        content = Files.readString(filePath);
                    }
                    Cid cid = Kad.genCid(content);

                    if (in_attrs.isEmpty()) {
                        //属性情報を指定していない場合
                        //通常のコンテンツPUT
                        List<PeerAddresses> list = ipfs.dht.putRawContent(content.getBytes(), cid, ipfs.node);
                        if (list.isEmpty()) {
                            cid = ipfs.blockstore.put(content.getBytes(), Cid.Codec.lookupIPLDName("raw")).join();

                        }
                        Map res = new HashMap<>();
                        int len = list.size();
                        for (int i = 0; i < len; i++) {
                            res.put("Addr" + i, list.get(i).toString());
                        }
                        res.put("CID_file", cid.toString());
                        replyJson(httpExchange, JSONParser.toString(res));
                    } else {
                        //属性情報を指定されている場合
                        //属性の指定されていれば，ファイル＋属性の双方をputする．
                        String attrs = in_attrs.get();
                        StringTokenizer token = new StringTokenizer(attrs, "-");
                        LinkedList<HashMap<String, String>> attrList = new LinkedList<HashMap<String, String>>();
                        while (token.hasMoreTokens()) {
                            HashMap<String, String> attrMap = new HashMap<String, String>();
                            String attr = token.nextToken();
                            //さらにデリミタで属性名/値を取得する．
                            StringTokenizer subToken = new StringTokenizer(attr, Kad.delimiter);
                            while (subToken.hasMoreTokens()) {
                                String name = subToken.nextToken();
                                String value = null;
                                if (subToken.hasMoreTokens()) {
                                    value = subToken.nextToken();

                                } else {
                                    throw new APIException("Attribute info. should be: " + "name" + Kad.delimiter + "value");

                                }
                                attrMap.put(name, value);

                            }
                            attrList.add(attrMap);

                        }

                        //属性＋コンテンツをputする．
                        List<PeerAddresses> list = ipfs.dht.putContentWithAttr(content.getBytes(), cid, attrList, ipfs.node);
                        if (list.isEmpty()) {
                            cid = ipfs.blockstore.put(content.getBytes(), Cid.Codec.lookupIPLDName("raw")).join();

                        }
                        Map res = new HashMap<>();
                        int len = list.size();
                        for (int i = 0; i < len; i++) {
                            res.put("Addr" + i, list.get(i).toString());
                        }
                        //res.put("Addr", addr.getPublicAddresses().toString());
                        res.put("CID_file", cid.toString());
                        replyJson(httpExchange, JSONParser.toString(res));
                    }

                    break;
                //CIDを指定してでの通常のコンテンツGET
                case GET_VALUE:
                    // curl -X POST "http://127.0.0.1:5001/api/v0/dht/getvalue?cid=1AYbp7Xk3AU6SoDF3tFNWyVLFHKSyJNca9Zcg6PXj3b2fj"
                    // /dht/getvalue?cid=xxxx&writepath=xxxx
                    //指定のcidにて値を取得する．FIND_PROVIDERSにてprovidersを取得し，それに対して値を要求する．
                    Optional<String> cidOpt = Optional.ofNullable(params.get("cid")).map(a -> a.get(0));
                    Optional<String> writePathOpt = Optional.ofNullable(params.get("writepath")).map(a -> a.get(0));
                    if (cidOpt.isEmpty()) {
                        throw new APIException("argument \"cid\" is required\n");

                    }
                    String get_cid = cidOpt.get();
                    Multihash cid_converted = Multihash.fromBase58(get_cid);
                    // Multihash.deserialize(cid_converted);
                    List<CborObject.CborMap> mapList = new ArrayList<CborObject.CborMap>();
                    mapList = ipfs.dht.getValueWithMerkleDAG(cid_converted, get_cid, ipfs.node).join();

                    //String val = val_tmp.get();
                    //String val = String.valueOf(ipfs.dht.getValueWithAttr(cid_converted, get_cid, ipfs.node));

                    //System.out.println("val:" + val);
                    mapList.add(mapList.get(0));

                    if (!mapList.isEmpty()) {

                        //System.out.println("val:" + val);
                        //replyBytes(httpExchange, val.getBytes());
                        //replyJson(httpExchange, JSONParser.toString(new String(Kad.getDataFromMerkleDAG(map))));

                        // ObjectMapper mapper = new ObjectMapper();
                        // String json = mapper.writeValueAsString(mapList);
                        StringBuffer buf = new StringBuffer();
                        Iterator<CborObject.CborMap> mIte = mapList.iterator();
                        while (mIte.hasNext()) {
                            CborObject.CborMap m = mIte.next();
                            buf.append("[value:");
                            buf.append(new String(Kad.getDataFromMerkleDAG(m)));
                            buf.append("],");
                        }

                        replyJson(httpExchange, JSONParser.toString(buf.toString()));


                    } else {
                        LOG.info("******NO DATA******");
                        try {
                            httpExchange.sendResponseHeaders(400, 0);
                        } catch (IOException ioe) {
                            HttpUtil.replyError(httpExchange, ioe);
                        }
                    }

                    break;
                //属性フィルタを，範囲指定してPUTする．
                //指定範囲での属性PUT(担当ノード用）
                case PUT_ATTRWITHRANGE:
                    //curl -X POST "http://127.0.0.1:5001/api/v0/dht/putattrs?attrname=time&min=08&max=10"
                    Optional<String> name = Optional.ofNullable(params.get("attrname")).map(a -> a.get(0));
                    Optional<String> str_min = Optional.ofNullable(params.get("min")).map(a -> a.get(0));
                    long min = Long.valueOf(str_min.get()).longValue();
                    Optional<String> str_max = Optional.ofNullable(params.get("max")).map(a -> a.get(0));
                    long max = Long.valueOf(str_max.get()).longValue();
                    Map resMap = new HashMap<>();
                    int cnt = 0;
                    String tmpValue = null;
                    //PeerAddresses predAddr = null;
                    ArrayList<HashMap<String, Object>> targetList = new ArrayList<HashMap<String, Object>>();


                    for (long i = min; i < max + 1; i++) {
                        HashMap<String, Object> map = new HashMap<String, Object>();
                        StringBuffer buf = new StringBuffer();
                        if ((i < 10) && (i >= 0)) {
                            buf.append("0");
                            buf.append(i);
                            tmpValue = buf.toString();
                        } else {
                            tmpValue = Long.valueOf(i).toString();
                        }

                        String orgValue = Kad.genAttrMask(name.get(), tmpValue);
                        map.put("attr", orgValue);
                        Cid cid_attr = Kad.genCid(orgValue);
                        map.put("cid", cid_attr);

                        List<PeerAddresses> list = new LinkedList<PeerAddresses>();
                        //まず，put先だけを決める．
                        List<PeerAddresses> retList = ipfs.dht.findPutTarget(cid_attr, ipfs.node);
                        map.put("peer", retList.get(0));
                        targetList.add(map);
                    }

                    //各属性値ごとにPUTする．
                    //for(long i = min; i < max + 1; i++){
                    for (long i = 0; i < targetList.size(); i++) {
                        HashMap<String, Object> map = targetList.get((int) i);
                        String orgValue = (String) map.get("attr");
                        Cid cid0 = (Cid) map.get("cid");
                        List<PeerAddresses> list = new LinkedList<PeerAddresses>();
                        HashMap<String, Object> predMap = null;
                        HashMap<String, Object> sucMap = null;
                        PeerAddresses sucAddr = null;
                        PeerAddresses predAddr = null;
                        PeerAddresses currentAddrs = (PeerAddresses) map.get("peer");
                        //list = ipfs.dht.putAttr(orgValue.getBytes(), cid, ipfs.node);

                        if (i == 0) {
                            //先頭は，predはnull
                            sucMap = targetList.get((int) i + 1);
                            sucAddr = (PeerAddresses) sucMap.get("peer");
                            list = ipfs.dht.putAttrWithTarget(orgValue.getBytes(), cid0, ipfs.node, currentAddrs, null, sucAddr);
                        } else if (i == targetList.size() - 1) {
                            //suc無し，predあり
                            predMap = targetList.get((int) (i - 1));
                            predAddr = (PeerAddresses) predMap.get("peer");
                            list = ipfs.dht.putAttrWithTarget(orgValue.getBytes(), cid0, ipfs.node, currentAddrs, predAddr, null);

                        } else {
                            predMap = targetList.get((int) (i - 1));
                            predAddr = (PeerAddresses) predMap.get("peer");
                            sucMap = targetList.get((int) (i + 1));
                            sucAddr = (PeerAddresses) sucMap.get("peer");
                            list = ipfs.dht.putAttrWithTarget(orgValue.getBytes(), cid0, ipfs.node, currentAddrs, predAddr, sucAddr);
                        }
                        cnt++;



            /*
            HashMap<String, Object> inMap = new HashMap<String, Object>();
            // res.put("Addr", addr.getPublicAddresses().toString());
            int len = list.size();
            for(int j=0;i<len;j++){
              inMap.put("Addr" + j, list.get(j).toString());
            }
            inMap.put("CID", cid.toString());
            inMap.put("value", i);
            resMap.put(i, inMap);
*/

                    }
                    replyJson(httpExchange, JSONParser.toString(cnt + " nodes are allocated"));


                    //String orgValue = attrName.get()  + "^" + attrValue.get();

                    break;
                //curl -X POST "http://127.0.0.1:5001/api/v0/dht/putattr?attrname=time&attrvalue=08"
                //この場合は，属性ペアのみPUTする．
                //dht/putattr?arg=CID&attrname=time&attrvalue=0825
                //この場合は，CIDのclosestPeerに対してCID + 属性ペアの2つをPUTする．
                //PUTされたときに，担当ピア -> PUT先ピアへリンクをはってもらう
                //一つの属性PUT(担当ノード用）
                //非推奨
                case PUT_ATTR:
                    Optional<String> attrName = Optional.ofNullable(params.get("attrname")).map(a -> a.get(0));
                    Optional<String> attrValue = Optional.ofNullable(params.get("attrvalue")).map(a -> a.get(0));
                    String orgValue = Kad.genAttrMask(attrName.get(), attrValue.get());
                    //Cid cid = ipfs.blockstore.put(orgValue.getBytes(), Cid.Codec.lookupIPLDName("raw")).join();
                    //Multihash hashedValue =  new Multihash(Multihash.Type.sha2_256, cid.getHash());
                    Cid cid2 = Kad.genCid(orgValue);


                    List<PeerAddresses> list = new LinkedList<PeerAddresses>();
                    list = ipfs.dht.putAttr(orgValue.getBytes(), cid2, ipfs.node);
                    if (list.isEmpty()) {
                        cid = ipfs.blockstore.put(orgValue.getBytes(), Cid.Codec.lookupIPLDName("raw")).join();
                    }

                    Map res = new HashMap<>();
                    // res.put("Addr", addr.getPublicAddresses().toString());
                    int len = list.size();
                    for (int i = 0; i < len; i++) {
                        res.put("Addr" + i, list.get(i).toString());
                    }
                    res.put("CID", cid2.toString());


                    replyJson(httpExchange, JSONParser.toString(res));

                    break;

                //findByAttrRange
                //属性値の範囲で検索する．
                //dht/findbyattrvals?attrname=time&attrvalmin=08&attrvalmax=11
                //として，timeが 08 ~ 11のコンテンツを取得する．
                //まずは08担当ピア検索するために，Hash(time^08)で検索する．08担当ピアについたら，
                //そこからtime^08*からリンクされているコンテンツリストを取得し，返す．
                //その後，08担当ピア -> 09担当ピアへ要求をforwardする．
                case FIND_ATTRS:
                    //値が文字列の場合は，別途考慮すべき
                    // curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_08_10-temp_25_35&cidonly=true"
                    // curl -X POST "http://127.0.0.1:5001/api/v0/dht/getbyattrs?attrs=time_09_10&cidonly=true"
                    // /dht/getvalue?cid=xxxx&writepath=xxxx
                    //指定のcidにて値を取得する．FIND_PROVIDERSにてprovidersを取得し，それに対して値を要求する．
                    //基本AND条件として，INT値を想定する．
                    //各属性で担当者が違うので，別々に問い合わせる必要がある．
                    //複数条件(time^08-10)というように，聞く．
                    //戻り値はアドレス戻り値のリスト＋cborリスト？
                    Optional<String> attrArray = Optional.ofNullable(params.get("attrs")).map(a -> a.get(0));
                    Optional<String> cidonly = Optional.ofNullable(params.get("cidonly")).map(a -> a.get(0));
                    boolean isCidOnly = false;
                    if(!cidonly.isEmpty()){
                        isCidOnly = true;
                    }

                    if (attrArray.isEmpty()) {
                        throw new APIException("argument \"attrs\" is required\n");
                    }
                    String attrInfo = attrArray.get();
                    StringTokenizer token = new StringTokenizer(attrInfo, "-");
                    HashMap<String, CborObject.CborMap> mergedMap = new HashMap<String, CborObject.CborMap>();
                    Map resultMap = new HashMap();
                    LinkedList<Map<String, Cborable>> retList = new LinkedList<Map<String, Cborable>>();

                    //属性単位のループ
                    while (token.hasMoreTokens()) {
                        HashMap tmpMap = new HashMap();

                        //一つの属性内のループ
                        String subAttr = token.nextToken();
                        //_で区切る．
                        StringTokenizer subToken = new StringTokenizer(subAttr, "_");
                        //名前
                        String key = subToken.nextToken();
                        //値
                        String strMin_tmp = subToken.nextToken();
                        int attrMin = Integer.parseInt(strMin_tmp);
                        //max
                        String strMax_tmp = subToken.nextToken();
                        int attrMax = Integer.parseInt(strMax_tmp);

                        //mask値を生成する．
                        String mask = Kad.genAttrMask(key, strMin_tmp);
                        //cidを生成する．
                        Cid attrCid = Kad.genCid(mask);
                       // List<CborObject.CborMap> attrList = new ArrayList<CborObject.CborMap>();
                        //attrCidは，cid(time^08)として，あくまでリクエストにセットするためのもの．
                        //実際にDBに対して検索するのは，strMin_tmpである．
                        CompletableFuture<CborObject.CborMap> retMap = ipfs.dht.getValueByAttrs(attrCid, key, strMin_tmp, strMax_tmp, ipfs.node, isCidOnly);
                        CborObject.CborMap map = retMap.get();
                        Iterator<String> kmap = map.keySet().iterator();

                      
                        //CIDごとのループ
                        while (kmap.hasNext()) {
                            String cid10 = kmap.next();
                            CborObject.CborMap cMap = (CborObject.CborMap) map.get(cid10);
                            Iterator<String> cKeyIte = cMap.keySet().iterator();
                            Map<String, String> newVal = new HashMap<String, String>();

                            while (cKeyIte.hasNext()) {
                                String cKey = cKeyIte.next();
                                Cborable val = cMap.get(cKey);
                                //ここで例外発生
                                //CborObject.CborString str_val = (CborObject.CborString)val;
                                //String str = str_val.value;]


                                newVal.put(cKey, val.toString());
                            }
                            tmpMap.put(cid10, cMap);
                            retList.add(tmpMap);
                            //resultMap.put(cid10, newVal);



                        }


                        //<cid, CborMap>のmapを取得する．

                        // Iterator<>
                        //List< CborObject.CborMap> mList = retList.get();


                    }
                    Iterator<Map<String, Cborable>> ite = retList.iterator();
                    while(ite.hasNext()){
                        Map<String, Cborable> map = ite.next();
                        Iterator<String> keyIte = map.keySet().iterator();
                        while(keyIte.hasNext()){
                            String fcid = keyIte.next();
                            if(this.isMapExistsAll(retList, fcid)){
                                resultMap.put(fcid, map);
                            }

                        }
                    }
                    //listに対するループ
                    replyJson(httpExchange, JSONParser.toString(resultMap));


                    break;
                default: {
                    httpExchange.sendResponseHeaders(404, 0);
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            HttpUtil.replyError(httpExchange, e);
        } finally {
            httpExchange.close();
            long t2 = System.currentTimeMillis();
            if (LOGGING)
                LOG.info("API Handler handled " + path + " query in: " + (t2 - t1) + " mS");
        }
    }

    public boolean isMapExistsAll(LinkedList<Map<String, Cborable>> retList, String cid){
        Iterator<Map<String, Cborable>> ite = retList.iterator();
        boolean isExists = true;
        while(ite.hasNext()){
            Map<String, Cborable> map = ite.next();
            if(map.containsKey(cid)){

            }else{
                isExists = false;
                break;
            }

        }
        return isExists;
    }
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           