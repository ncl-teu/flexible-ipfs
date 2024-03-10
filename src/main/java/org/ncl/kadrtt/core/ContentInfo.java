package org.ncl.kadrtt.core;

import java.io.Serializable;

public class ContentInfo implements Serializable {
    private String cid;

    /**
     * アクセス数
     */
    private long count;

    /**
     * 登録日時
     * すでにフォルダに有る場合は，フォルダから読み取った日時
     */
    private long registerDate;

    /**
     * 前回のアクセス確率
     */
    private double prevLookupProbability;


    public ContentInfo(String cid) {
        this.cid = cid;
        this.count = 0;
        this.registerDate = System.currentTimeMillis();
        this.prevLookupProbability = 0.0;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getRegisterDate() {
        return registerDate;
    }

    public void setRegisterDate(long registerDate) {
        this.registerDate = registerDate;
    }

    public double getPrevLookupProbability() {
        return prevLookupProbability;
    }

    public void setPrevLookupProbability(double prevLookupProbability) {
        this.prevLookupProbability = prevLookupProbability;
    }
}
