package org.ncl.kadrtt.core.cmds;

import com.jcraft.jsch.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.jcraft.jsch.ChannelSftp.OVERWRITE;

public class ControlMain {
    public static void main(String[] args){
        if(args.length == 0){
            System.out.println("Please input argument.");
            System.exit(1);
        }
        //引数 = コマンドを取得する．
        String param = args[0];
        ArrayList<HostInfo> hostList = new ArrayList<HostInfo>();

        try{
            //peerlistファイルの読み込みを行う．
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("peerlist"), "UTF-8"));
            // 最終行まで読み込む
            String line = "";

            //IPアドレス,ユーザ名,パスワード,nabuのディレクトリ
            while ((line = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line, ",");
                int cnt = 0;
                while (st.hasMoreTokens()) {
                    // 1行の各要素をタブ区切りで表示
                    String ip_addr = st.nextToken();
                    String userName = st.nextToken();
                    String password = st.nextToken();
                    String dir = st.nextToken();
                    HostInfo info = new HostInfo(ip_addr,userName, password, dir);
                    hostList.add(info);
                }
            }

        int len = hostList.size();
            //行単位(=ノード)のループ
        for(int i=0;i<len;i++){
            HostInfo node = hostList.get(i);

            JSch jsch = new JSch();
            Session session = jsch.getSession(node.getUser(), node.getIp(), 22);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(node.getPassowrd());
            session.connect();

            String command = null;

            switch (param){
                //ノード一斉起動
                case "start":
                 //  command = "cd " + node.getPath() + "|pwd";
                   // command = "ip a&";
                    command = node.getPath() + "/run.sh &";
                    break;
                //ノード一斉停止
                case "stop":
                    command = "curl -X POST \"http://127.0.0.1:5001/api/v0/exit\"";
                    break;
                case "simstart":
                    command = node.getPath() + "/simrun.sh &";
                    break;
                case "simstop":
                    command = "curl -X POST \"http://127.0.0.1:5001/api/v0/exit\"|killall java";
                    break;
                //DBの初期化
                case "dbinit":
                    command = "curl -X POST \"http://127.0.0.1:5001/api/v0/dht/inittable\"";
                    break;
                //モジュール一斉反映
                case "up":
                    //lib, classes, kadrtt.properties, *.sh providers getdata
                   ChannelSftp channel  = (ChannelSftp) session.openChannel("sftp");
                    channel.connect();
                    Path p1 = Paths.get("");
                    Path p2 = p1.toAbsolutePath();
                    //コピーするファイルとディレクトリを指定する．
                    ControlMain.putDir(session, p2 + "/classes", node.getPath() + "/classes");
                    ControlMain.putDir(session, p2 + "/lib", node.getPath()+"/lib");
                    channel.put(p2 + "/*.sh", node.getPath(), OVERWRITE);
                    channel.chmod(0755, node.getPath() + "/*.sh");
                    channel.put(p2 + "/*.properties", node.getPath(), OVERWRITE);
                    channel.chmod(0777, node.getPath() + "/*.properties");
                    channel.put(p2 + "/cid.csv", node.getPath() + "/cid.csv", OVERWRITE);

                    break;

            }
            if(!param.equals("up")){
                CmdThread cmdt = new CmdThread(session, command);
                Thread t = new Thread(cmdt);
                t.start();
                Thread.sleep(10);
                /*
                ChannelExec channel = (ChannelExec) session.openChannel("exec");
                channel.setCommand(command);
                InputStream in = channel.getInputStream();
                channel.connect();

                Scanner s = new Scanner(in).useDelimiter("\\A");
                String result = s.hasNext() ? s.next() : "";

                s.close();
                channel.disconnect();
                // コマンドの戻り値を取得する
                int returnCode = channel.getExitStatus();

                 */
            }

            System.out.println("Host:" + node.getIp() + ":" + param + " Completed");

        }
        System.out.println("**All nodes completed!!***");
        //System.exit(1);



        }catch(Exception e){
            e.printStackTrace();
        }

    }


    public  static void putDir( Session session, String localDirectory, String remoteTargetDirectory) throws IOException {
        File curDir = new File(localDirectory);
        final String[] fileList = curDir.list();
        try{
            ChannelSftp channel  = (ChannelSftp) session.openChannel("sftp");
            channel.connect();
            try{
                channel.lstat(remoteTargetDirectory);
            }catch(Exception e){
                ChannelExec channel2 = (ChannelExec) session.openChannel("exec");
                channel2.setCommand("mkdir " + remoteTargetDirectory);
                channel2.connect();
                channel2.disconnect();
            }
            //channel.lstat(remoteTargetDirectory).


           /* ChannelExec channel_root = (ChannelExec) session.openChannel("exec");
            channel_root.setCommand("mkdir " + remoteTargetDirectory);
            channel_root.connect();
            channel_root.disconnect();
*/
            for (String file : fileList) {
                final String fullFileName = localDirectory + "/" + file;
                ChannelExec channel2 = (ChannelExec) session.openChannel("exec");
                if (new File(fullFileName).isDirectory()) {

                    final String subDir = remoteTargetDirectory + "/" + file;
                    channel2.setCommand("mkdir " + subDir);
                    channel2.connect();
                    channel2.disconnect();

                    ControlMain.putDir(session, fullFileName, subDir);

                } else {
                    //ローカルファイルのmtimeを取得
                    FileTime fileTime = Files.getLastModifiedTime(Paths.get(fullFileName));
                    long localMTime = fileTime.to(TimeUnit.SECONDS);
                    try{
                        //channel.ls(remoteTargetDirectory + "/" + file);
                        channel.lstat(remoteTargetDirectory + "/" + file);
                    }catch(Exception e){
                        //タイムスタンプのチェックを行う．
                        channel.put(fullFileName, remoteTargetDirectory, OVERWRITE);
                        channel.setMtime(remoteTargetDirectory + "/" + file, (int)localMTime);
                    }
                    //リモートファイルのmtimeを取得
                    SftpATTRS attrs = channel.lstat(remoteTargetDirectory + "/" + file);
                    int remoteMTime = attrs.getMTime();


                    //もしローカルが新しければ，putを行った上でリモートファイルのmtimeをローカルファイルのmtimeにする．
                    if(localMTime > remoteMTime){
                        //タイムスタンプのチェックを行う．
                        channel.put(fullFileName, remoteTargetDirectory, OVERWRITE);
                        channel.setMtime(remoteTargetDirectory + "/" + file, (int)localMTime);
                    }

                }
            }
            channel.disconnect();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

}
