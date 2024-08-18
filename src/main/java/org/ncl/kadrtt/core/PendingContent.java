package org.ncl.kadrtt.core;

import org.ncl.kadrtt.core.chunk.AbstractChunkProvider;
import org.peergos.cbor.CborObject;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;

public class PendingContent implements Serializable {

    private String cid;

    /**
     * 未完了のチャンクCID集合
     */
    private LinkedHashSet<String> set;




    /**
     * チャンクの全リスト
     */
    private ArrayList<CborObject.CborString> cidList;

    public PendingContent(String in_cid, LinkedHashSet<String> in_set,
                          ArrayList<CborObject.CborString> in_cidList){
        this.cid = in_cid;
        this.set = in_set;
        this.cidList = in_cidList;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public LinkedHashSet<String> getSet() {
        return set;
    }

    public void setSet(LinkedHashSet<String> set) {
        this.set = set;
    }

    public ArrayList<CborObject.CborString> getCidList() {
        return cidList;
    }

    public void setCidList(ArrayList<CborObject.CborString> cidList) {
        this.cidList = cidList;
    }
}
