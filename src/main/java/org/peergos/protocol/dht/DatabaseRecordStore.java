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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DatabaseRecordStore implements RecordStore, AutoCloseable {

    private final String connectionStringPrefix = "jdbc:h2:";//./store/records;AUTO_RECONNECT=TRUE
    private final Connection connection;

    private final String RECORD_TABLE = "records";

    private final String PREDSUC_TABLE = "predsuc";

    private final String ATTRLINK_TABLE = "attrlink";

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
     * @param cid
     * @param attr
     * @param addr
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
    public HashMap<String, HashMap<String, AttrBean>> getAttrLink(String attrInfo){

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

