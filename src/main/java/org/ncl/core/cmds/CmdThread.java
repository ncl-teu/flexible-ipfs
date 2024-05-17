package org.ncl.kadrtt.core.cmds;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Session;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

public class CmdThread implements Runnable{
    private Session session;
    private String command;

    @Override
    public void run() {
        try{
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            //InputStream in = channel.getInputStream();
            channel.connect(5000);
            // エラーメッセージ用Stream
            BufferedInputStream errStream = new BufferedInputStream(channel.getErrStream());
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            while (true) {
                int len = errStream.read(buf);
                if (len <= 0) {
                    break;
                }
                outputStream.write(buf, 0, len);
            }
            // エラーメッセージ取得する
            String message = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
            System.out.println("**ERRR MSG: "+ message);


           // Scanner s = new Scanner(in).useDelimiter("\\A");
          //  String result = s.hasNext() ? s.next() : "";

           // s.close();
            //channel.disconnect();
            // コマンドの戻り値を取得する
            //int returnCode = channel.getExitStatus();
            //System.out.println(result);

        }catch(Exception e){
            e.printStackTrace();
        }

    }


    public CmdThread(Session session, String cmd) {
        this.session = session;
        this.command = cmd;
    }
}
