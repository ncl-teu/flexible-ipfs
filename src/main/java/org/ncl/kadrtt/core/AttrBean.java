package org.ncl.kadrtt.core;

import org.peergos.cbor.CborObject;
import org.peergos.cbor.Cborable;

import java.io.Serializable;

public class AttrBean  implements Serializable {

   // private String peerId;

    private String cid;

    private String attrMask;

    private String addr;

    public AttrBean(/*String pid, */String cid, String attrMask, String addrMask) {
        //this.peerId = pid;
        this.cid = cid;
        this.attrMask = attrMask;
        this.addr = addrMask;
    }


/*
    public String getPeerId() {
        return peerId;
    }

    public void setPeerId(String peerId) {
        this.peerId = peerId;
    }
*/
    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getAttrMask() {
        return attrMask;
    }

    public void setAttrMask(String attrMask) {
        this.attrMask = attrMask;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddrMask(String addrMask) {
        this.addr = addrMask;
    }
}
