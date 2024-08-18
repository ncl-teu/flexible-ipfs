package org.ncl.kadrtt.core.cmds;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Test {
    public static void main(String[] args){
        String s = "test.mp4";
        int idx = s.lastIndexOf(".");
        if(idx == -1){
            System.out.println("fdsfa");
        }
        String sub = s.substring(idx+1);
        System.out.println(sub);
    }
}
