package org.ncl.kadrtt.core.cmds;

import java.io.*;

public class LogThread implements Runnable{
    private InputStream in;
    private String type;

    public LogThread(InputStream in) {
        this.in = in;

    }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(in, "MS932"))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                // ログなど出力する
                //ここで，ファイルに書き出しておく必要がある．
                System.out.println(line);
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
