package org.ncl.kadrtt.core.cmds;

import java.io.Serializable;

public class HostInfo implements Serializable {
    private String ip;

    private String user;

    private String passowrd;

    private String path;


    public HostInfo(String ip, String user, String passowrd, String path) {
        this.ip = ip;
        this.user = user;
        this.passowrd = passowrd;
        this.path = path;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassowrd() {
        return passowrd;
    }

    public void setPassowrd(String passowrd) {
        this.passowrd = passowrd;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
