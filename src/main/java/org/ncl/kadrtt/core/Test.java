package org.ncl.kadrtt.core;

import org.peergos.cbor.CborObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.stream.Stream;

public class Test {

    public static void main(String[] args){
        try {
            String cid = "abcde";
            String cid2 = "xyz";
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();

            String abs = p2 + File.separator + "getdata";
            File dir = new File(abs);

            File[] files = dir.listFiles();
            int len  = files.length;
            for(int i=0;i<len;i++){
               // System.out.println(files[i].getName());
                if(files[i].isDirectory()){
                    System.out.println(files[i].getName());
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }


    }
}
