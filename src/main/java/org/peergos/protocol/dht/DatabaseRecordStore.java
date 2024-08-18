package org.peergos.protocol.dht;
import io.ipfs.cid.Cid;
import io.ipfs.multibase.binary.Base32;
import io.ipfs.multihash.Multihash;
import org.ncl.kadrtt.core.AttrBean;
import org.peergos.PeerAddresses;
import org.peergos.protocol.ipns.IpnsRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

public class DatabaseRecordStore implements RecordStore, AutoCloseable {

    private final String connectionStringPrefix = "jdbc:h2:";//./store/records;AUTO_RECONNECT=TRUE
    private final Connection connection;

    private final String RECORD_TABLE = "records";

    private final String PREDSUC_TABLE = "predsuc";

    private final String ATTRLINK_TABLE = "attrlink";

    private final String TAG_TABLE = "tags";

    private final String MGR_TABLE = "mgrs";

    private final String CONTENT_POPULARITY = "contentpopularity";

    private final String ATTR_TABLE = "attrs";
    private final int SIZE_OF_VAL = 10 * 1024; // 10KiB
    private final int SIZE_OF_PEERID = 100;

    /*
     * Constructs a DatabaseRecordStore object
     * @param location - location of the database on disk (See: https://h2database.com/html/cheatSheet.html for options)
     */
    public DatabaseRecordStore(String location) {
        try {
            this.connection = getConnection(connectionStringPrefix + location + ";DB_CLOSE_ON_EXIT=TRUE");
            //+ ";DB_CLOSE_ON_EXIT=TRUE"
            //this.connection = DriverManager.getConnection(connectionStringPrefix + "tcp://localhost/" + location);

            this.connection.setAutoCommit(true);
            createTable();
            createPredSucTable();
            createAttrLinkTable();
            createTagTable();
            createMgrTable();

        } catch (SQLException sqle) {
            throw new IllegalStateException(sqle);
        }
    }
    public void close() throws Exception {
        connection.close();
    }

    private Connection getConnection(String connectionString) throws SQLException {

        return DriverManager.getConnection(connectionString);
    }

    private void createTable() {
        String createSQL = "create table if not exists " + RECORD_TABLE
                + " (peerId VARCHAR(" + SIZE_OF_PEERID + ") primary key not null, raw BLOB not null, "
                + "sequence BIGINT not null, ttlNanos BIGINT not null, expiryUTC BIGINT not null, "
                + "val VARCHAR(" + SIZE_OF_VAL + ") not null);";
        try (PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     *
     * @param cid  CID(属性名^属性値)
     * @param attr  属性名^属性値
     * @param pred 先行ノードのPeerID
     * @param suc  後続ノードのPeerID
     */
    public void putPredSuc(String cid, String attr, String pred, String suc) {

        String updateSQL = "MERGE INTO " + PREDSUC_TABLE
                + " (cid, attr, pred, suc) VALUES (?,?, ?, ?);";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, cid);
            pstmt.setString(2, attr);
            if(pred == null){
                pstmt.setNull(3, Types.NULL);
            }else{
                pstmt.setString(3, pred);

            }
            if(suc == null){
                pstmt.setNull(4, Types.NULL);
            }else{
                pstmt.setString(4, suc);

            }

            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    /**
     *
     */
    public void dropTables() {
       String dropPredSuc = "delete from " + PREDSUC_TABLE;
        try (PreparedStatement select = connection.prepareStatement(dropPredSuc)) {
            select.executeUpdate();

            //同様に，attrlinkも削除する．
            String attrLink = "delete from " + ATTRLINK_TABLE;
            PreparedStatement select2 = connection.prepareStatement(attrLink);
            select2.executeUpdate();

            //同様に，attrlinkも削除する．
            String tags = "delete from " + TAG_TABLE;
            PreparedStatement select3 = connection.prepareStatement(tags);
            select3.executeUpdate();

            String mgrs = "delete from " + MGR_TABLE;
            PreparedStatement select4 = connection.prepareStatement(tags);
            select.executeUpdate();



        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }

    }


    /**
     *
     * @param mask
     * @param maskcid
     * @param addr
     */
    public void putMgr(String mask, String maskcid, String addr) {
        String selectSQL = "SELECT * FROM " + MGR_TABLE
                + " WHERE mask=?;";

        try (PreparedStatement pstmt0 = connection.prepareStatement(selectSQL)) {
            pstmt0.setString(1, mask);
            //pstmt0.setString(2, addr);

            String ret_str = null;
            String ret_cid = null;
            LinkedList<String> list = new LinkedList<String>();
            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            try (ResultSet rs = pstmt0.executeQuery()) {
                //リストにデータを追加する
                while (rs.next()) {
                    list.add(rs.getString("maskcid"));
                }
                if(!list.isEmpty()){
//なければ追加する．
                    String updateSQL = "UPDATE " + MGR_TABLE
                            + " SET addr=? WHERE mask=?;";
                    try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
                        pstmt.setString(1, addr);
                        pstmt.setString(2, mask);
                       // pstmt.setString(3, addr);
                        pstmt.executeUpdate();
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }else{
                    //なければ追加する．
                    String updateSQL = "INSERT INTO " + MGR_TABLE
                            + " (mask, maskcid, addr) VALUES (?, ?, ?);";
                    try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
                        pstmt.setString(1, mask);
                        pstmt.setString(2, maskcid);
                        pstmt.setString(3, addr);
                        pstmt.executeUpdate();
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
            }



        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }

    }


    /**
     *
     * @param mask
     * @return
     */
    public String getMgr(String mask){

        HashSet<String> addrSet = new HashSet<String>();

        String selectSQL = "SELECT * FROM " + MGR_TABLE
                + " WHERE mask=?;";
        String ret_addr = null;

        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, mask);
            String ret_cid = null;
            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ret_addr = rs.getString("addr");
                    //addrSet.add(ret_cid);
                }
            }catch(Exception e){
                e.printStackTrace();
            }

            return ret_addr;

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void createMgrTable() {
       /* String dropSQL = "drop table " + PREDSUC_TABLE;
        try (PreparedStatement select = connection.prepareStatement(dropSQL)) {
            select.executeUpdate();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }*/
        String createSQL = "create table if not exists " + MGR_TABLE
                + "(id BIGINT auto_increment,"
                + " mask  VARCHAR(255) not null,"
                + " maskcid VARCHAR(255) not null, "
                + " addr VARCHAR(65535));";
        try (PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    private void createTagTable() {
       /* String dropSQL = "drop table " + PREDSUC_TABLE;
        try (PreparedStatement select = connection.prepareStatement(dropSQL)) {
            select.executeUpdate();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }*/
        String createSQL = "create table if not exists " + TAG_TABLE
                + "(id BIGINT auto_increment,"
                + " tagcid  VARCHAR(255) not null,"
                + " taginfo VARCHAR(255) not null, "
                + " tagname VARCHAR(255),"
                + " tagvalue VARCHAR(255),"
                + " cid VARCHAR(255));";
        try (PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    /**
     *
     * @param tagCid
     * @param tagInfo
     * @param tagName
     * @param tagValue
     * @param cid
     */
    public void putTagInfo(String tagCid, String tagInfo, String tagName, String tagValue, String cid) {

        String selectSQL = "SELECT * FROM " + TAG_TABLE
                + " WHERE tagcid=? and cid=?;";

        try (PreparedStatement pstmt0 = connection.prepareStatement(selectSQL)) {
            pstmt0.setString(1, tagCid);
            pstmt0.setString(2, cid);

            String ret_str = null;
            String ret_cid = null;
            LinkedList<String> list = new LinkedList<String>();
            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            try (ResultSet rs = pstmt0.executeQuery()) {
                //リストにデータを追加する
                while (rs.next()) {
                    list.add(rs.getString("cid"));
                }

//リストがからの場合、nullを返却する
                if(!list.isEmpty()){

                /*if (rs.next()) {
                    ret_cid = rs.getString("cid");
                    //もしあれば何もしない*/
                }else{
                    //なければ追加する．
                    String updateSQL = "INSERT INTO " + TAG_TABLE
                            + " (tagcid, taginfo, tagname, tagvalue, cid) VALUES (?, ?, ?, ?, ?);";
                    try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
                        pstmt.setString(1, tagCid);
                        pstmt.setString(2, tagInfo);
                        pstmt.setString(3, tagName);
                        pstmt.setString(4, tagValue);
                        pstmt.setString(5, cid);
                        pstmt.executeUpdate();
                    } catch (SQLException ex) {
                        throw new IllegalStateException(ex);
                    }
                }
            }catch(Exception e){
                e.printStackTrace();
            }



        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }

    }


    /**
     * タグ値による完全一致検索
     * @param tagcid
     * @return
     */
    public HashSet<String> getTagInfo(String tagcid){

        HashSet<String> cidSet = new HashSet<String>();

        String selectSQL = "SELECT cid FROM " + TAG_TABLE
                + " WHERE tagcid=?;";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, tagcid);
            String ret_str = null;
            String ret_cid = null;
            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ret_cid = rs.getString("cid");
                    cidSet.add(ret_cid);
                }
            }catch(Exception e){
                e.printStackTrace();
            }

            return cidSet;

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    /**
     * 担当ノード間の双方向リンクのテーブル
     * 属性名^属性値，pred / sucのPeerAddress or IPアドレスを格納する．
     */
    private void createPredSucTable() {
       /* String dropSQL = "drop table " + PREDSUC_TABLE;
        try (PreparedStatement select = connection.prepareStatement(dropSQL)) {
            select.executeUpdate();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }*/
        String createSQL = "create table if not exists " + PREDSUC_TABLE
                + " (cid  VARCHAR(" + SIZE_OF_PEERID + ") primary key not null,"
                + " attr VARCHAR(255) not null, "
                + " pred VARCHAR(65535),"
                + " suc VARCHAR(65535));";
        try (PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * 担当ノード間の双方向リンクのテーブル
     * コンテンツのCID，属性名^属性値，pred / sucのPeerAddress or IPアドレスを格納する．
     *
     */
    private void createAttrLinkTable() {
       /* String dropSQL = "drop table " + ATTRLINK_TABLE;
        try (PreparedStatement select = connection.prepareStatement(dropSQL)) {
            select.executeUpdate();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }*/
        String createSQL = "create table if not exists " + ATTRLINK_TABLE
                + " (id INT primary key AUTO_INCREMENT,"
                + " cid VARCHAR(" + SIZE_OF_PEERID + ") not null,"
                + " attr VARCHAR(255) not null, "
                + " addr VARCHAR(65535));";
        try (PreparedStatement select = connection.prepareStatement(createSQL)) {
            select.execute();

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * @param cid  CID(属性名^属性値)
     * @param attr  属性名^属性値
     * @param addr コンテンツのCID
     */
    public void putAttrLink(String cid, String attr, String addr) {

        String updateSQL = "INSERT INTO " + ATTRLINK_TABLE
                + " (cid, attr, addr) VALUES (?, ?, ?);";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, cid);
            pstmt.setString(2, attr);
            pstmt.setString(3, addr);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public boolean isInCharge(String attrMask){
        String selectSQL = "SELECT * FROM " + PREDSUC_TABLE
                + " WHERE attr=?;";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, attrMask);
            String ret_str = null;
            String ret_cid = null;
            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            try (ResultSet rs = pstmt.executeQuery()) {

                if (rs.next()) {
                    return true;
                }else{
                    return false;
                }
            }catch(Exception e){
                e.printStackTrace();
            }


        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }finally{

        }
        return false;
    }
    /**
     *      * 現在の属性情報から，後続担当者のpeerIDを取得する．
     * @param attrInfo 例: time^08
     * @return
     */
    public HashMap<String, String> getSuc(String attrInfo){

        String selectSQL = "SELECT suc, cid FROM " + PREDSUC_TABLE
                + " WHERE attr=?;";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, attrInfo);
            String ret_str = null;
            String ret_cid = null;
            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    ret_str = rs.getString("suc");
                    ret_cid = rs.getString("cid");
                }
            }catch(Exception e){
                e.printStackTrace();
            }
            //ResultSet rs = pstmt.executeQuery();
            HashMap<String, String> attrMap = new HashMap<String, String>();

            attrMap.put("suc", ret_str);
            attrMap.put("cid", ret_cid);
            //attrMapを用いて，指定属性値のコンテンツcborを取得する．
            return attrMap;

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }



    /**
     * 属性情報(time^09)から，紐づいているLEAF集合を取得する．
     * @param attrInfo 例: time^08
     * @return
     */
    public LinkedList<AttrBean> getAttrLink(String attrInfo){

        String selectSQL = "SELECT  cid, attr, addr FROM " + ATTRLINK_TABLE
                + " WHERE attr LIKE ?;";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, attrInfo + "%");

            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            ResultSet rs = pstmt.executeQuery();
            //HashMap<String, HashMap<String, AttrBean>> attrMap = new HashMap<String, HashMap<String, AttrBean>>();
            LinkedList<AttrBean> retList = new LinkedList<AttrBean>();

            while(rs.next()){
                //コンテンツのCIDを取得する．
                String content_cid = rs.getString("addr");
               // int idx = str_addr.indexOf(":");

                String cid = rs.getString("cid");
                AttrBean bean = new AttrBean(/*peerId, */cid, attrInfo, content_cid);
                retList.add(bean);


            }
            //attrMapを用いて，指定属性値のコンテンツcborを取得する．
            return retList;

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * 属性情報(time^09)から，紐づいているLEAF集合を取得する．
     * @return
     */
   /* public HashMap<String, HashMap<String, AttrBean>> getAttrLinkBak(String attrInfo){

        //LIKE演算子が良い？
        String selectSQL = "SELECT  cid, attr, addr FROM " + ATTRLINK_TABLE
                + " WHERE attr=?;";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, attrInfo);

            //Map: (peerId, Map<CID, beans(属性mask, addr_str>)
            ResultSet rs = pstmt.executeQuery();
            HashMap<String, HashMap<String, AttrBean>> attrMap = new HashMap<String, HashMap<String, AttrBean>>();

            while(rs.next()){
                String str_addr = rs.getString("addr");
                int idx = str_addr.indexOf(":");
                String peerId = null;
                if(idx != -1){
                    peerId = str_addr.substring(0, idx-1);
                }else{
                    peerId = str_addr;
                }
                String cid = rs.getString("cid");
                AttrBean bean = new AttrBean(peerId, cid,
                        rs.getString("attr"), rs.getString("addr"));

                if(attrMap.containsKey(peerId)){
                    HashMap<String, AttrBean> map = attrMap.get(peerId);
                    map.put(cid, bean);

                }else{
                    HashMap<String, AttrBean> subAttrMap = new HashMap<String, AttrBean>();
                    subAttrMap.put(rs.getString("cid"), bean);
                    attrMap.put(peerId, subAttrMap);
                }

            }
            //attrMapを用いて，指定属性値のコンテンツcborを取得する．
            return attrMap;

        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }
*/


    private String hashToKey(Multihash hash) {
        String padded = new Base32().encodeAsString(hash.toBytes());
        int padStart = padded.indexOf("=");
        return padStart > 0 ? padded.substring(0, padStart) : padded;
    }

    /**
     * /ipfs/Hash値　という形式で，レコードがあるかどうかをチェックする．
     * @param value
     * @return
     */
    public Optional<IpnsRecord> getByValue(String value){
        String selectSQL = "SELECT raw, sequence, ttlNanos, expiryUTC, val FROM " + RECORD_TABLE + " WHERE val=?";
        String refVal = null;
        if(value.length() > SIZE_OF_VAL){
            refVal = value.substring(0, SIZE_OF_VAL);
        }else{
            refVal = value;
        }

        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, refVal);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream input = rs.getBinaryStream("raw")) {
                        byte[] buffer = new byte[1024];
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        for (int len; (len = input.read(buffer)) != -1; ) {
                            bout.write(buffer, 0, len);
                        }
                        LocalDateTime expiry = LocalDateTime.ofEpochSecond(rs.getLong("expiryUTC"),
                                0, ZoneOffset.UTC);
                        IpnsRecord record = new IpnsRecord(bout.toByteArray(), rs.getLong("sequence"),
                                rs.getLong("ttlNanos"),  expiry, rs.getString("val"));
                        return Optional.of(record);
                    } catch (IOException readEx) {
                        throw new IllegalStateException(readEx);
                    }
                } else {
                    return Optional.empty();
                }
            } catch (SQLException rsEx) {
                throw new IllegalStateException(rsEx);
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }
    @Override
    public Optional<IpnsRecord> get(Cid peerId) {
        String selectSQL = "SELECT raw, sequence, ttlNanos, expiryUTC, val FROM " + RECORD_TABLE + " WHERE peerId=?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, hashToKey(peerId));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream input = rs.getBinaryStream("raw")) {
                        byte[] buffer = new byte[1024];
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        for (int len; (len = input.read(buffer)) != -1; ) {
                            bout.write(buffer, 0, len);
                        }
                        LocalDateTime expiry = LocalDateTime.ofEpochSecond(rs.getLong("expiryUTC"),
                                0, ZoneOffset.UTC);
                        IpnsRecord record = new IpnsRecord(bout.toByteArray(), rs.getLong("sequence"),
                                rs.getLong("ttlNanos"),  expiry, rs.getString("val"));

                        return Optional.of(record);
                    } catch (IOException readEx) {
                        throw new IllegalStateException(readEx);
                    }
                } else {
                    return Optional.empty();
                }
            } catch (SQLException rsEx) {
                throw new IllegalStateException(rsEx);
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }

    public Optional<IpnsRecord> get(String key) {
        String selectSQL = "SELECT raw, sequence, ttlNanos, expiryUTC, val FROM " + RECORD_TABLE + " WHERE peerId=?";
        try (PreparedStatement pstmt = connection.prepareStatement(selectSQL)) {
            pstmt.setString(1, hashToKey(Multihash.fromBase58(key)));
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    try (InputStream input = rs.getBinaryStream("raw")) {
                        byte[] buffer = new byte[1024];
                        ByteArrayOutputStream bout = new ByteArrayOutputStream();
                        for (int len; (len = input.read(buffer)) != -1; ) {
                            bout.write(buffer, 0, len);
                        }
                        LocalDateTime expiry = LocalDateTime.ofEpochSecond(rs.getLong("expiryUTC"),
                                0, ZoneOffset.UTC);
                        IpnsRecord record = new IpnsRecord(bout.toByteArray(), rs.getLong("sequence"),
                                rs.getLong("ttlNanos"),  expiry, rs.getString("val"));
                        return Optional.of(record);
                    } catch (IOException readEx) {
                        throw new IllegalStateException(readEx);
                    }
                } else {
                    return Optional.empty();
                }
            } catch (SQLException rsEx) {
                throw new IllegalStateException(rsEx);
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException(sqlEx);
        }
    }

    @Override
    public void put(Multihash peerId, IpnsRecord record) {

        String updateSQL = "MERGE INTO " + RECORD_TABLE
                + " (peerId, raw, sequence, ttlNanos, expiryUTC, val) VALUES (?, ?, ?, ?, ?, ?);";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, hashToKey(peerId));
            pstmt.setBytes(2, record.raw);
            pstmt.setLong(3, record.sequence);
            pstmt.setLong(4, record.ttlNanos);
            pstmt.setLong(5, record.expiry.toEpochSecond(ZoneOffset.UTC));
            pstmt.setString(6, record.value.length() > SIZE_OF_VAL ?
                    record.value.substring(0, SIZE_OF_VAL) : record.value);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void put(String key, IpnsRecord record) {
        String updateSQL = "MERGE INTO " + RECORD_TABLE
                + " (peerId, raw, sequence, ttlNanos, expiryUTC, val) VALUES (?, ?, ?, ?, ?, ?);";
        try (PreparedStatement pstmt = connection.prepareStatement(updateSQL)) {
            pstmt.setString(1, hashToKey(Multihash.fromBase58(key)));
            pstmt.setBytes(2, record.raw);
            pstmt.setLong(3, record.sequence);
            pstmt.setLong(4, record.ttlNanos);
            pstmt.setLong(5, record.expiry.toEpochSecond(ZoneOffset.UTC));
            pstmt.setString(6, record.value.length() > SIZE_OF_VAL ?
                    record.value.substring(0, SIZE_OF_VAL) : record.value);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }


    @Override
    public void remove(Multihash peerId) {
        String deleteSQL = "DELETE FROM " + RECORD_TABLE + " WHERE peerId=?";
        try (PreparedStatement pstmt = connection.prepareStatement(deleteSQL)) {
            pstmt.setString(1, hashToKey(peerId));
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }
}

