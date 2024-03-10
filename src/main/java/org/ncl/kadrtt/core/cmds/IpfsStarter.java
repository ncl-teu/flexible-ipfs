package org.ncl.kadrtt.core.cmds;

import org.ncl.kadrtt.core.Kad;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class IpfsStarter implements Runnable{

    private IpfsThread mainThread;

    public IpfsStarter(IpfsThread t){
        this.mainThread = t;
    }
    @Override
    public void run() {
        try{
            Path p1 = Paths.get("");
            Path p2 = p1.toAbsolutePath();
            System.out.println("CURRENT:" + p2);

           // ProcessBuilder a = new ProcessBuilder("cd ~/nabu-master");
            //Process c = a.start();


            //Runtime r = Runtime.getRuntime();
            //Process p = r.exec("route print");
            //コマンドのリスト
            //java -versionであれば，cmdList(0)が"java" cmdList(1)が"-version"
            List<String> cmdList = new LinkedList<String>();

            cmdList.add("java");
            cmdList.add("-cp");
            cmdList.add(Kad.CP);
            cmdList.add("org.peergos.APIServer");
            cmdList.add("Addresses.API");
            cmdList.add("/ip4/127.0.0.1/tcp/5001");
            //cmdList.add(">execResult");


            ProcessBuilder b = new ProcessBuilder(cmdList);
            Map<String, String> env = b.environment();
            env.put("HOME", ".");
            env.put("IPFS_HOME", ".ipfs");

            Process p = b.start();

            //プロセスが終了するのを待つ．
            LogThread lt = new LogThread(p.getInputStream());
            LogThread lt2 = new LogThread(p.getErrorStream());
            Thread tlog = new Thread(lt);
            Thread tlog2 = new Thread(lt2);
            tlog.start();
            tlog2.start();



            p.waitFor();
            this.mainThread.setStartCompleted(true);
            int code = p.exitValue();
           // System.out.println(code);
            //出力結果を得る．
         /*   BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                            p.getInputStream(),
                            Charset.defaultCharset()
                    )
            );
            BufferedReader reader2 = new BufferedReader(
                    new InputStreamReader(
                            p.getErrorStream(),
                            Charset.defaultCharset()
                    )
            );

            StringBuffer buf = new StringBuffer();
            String line;
            //出力結果を最終行まで読み込む
            while((line = reader.readLine()) != null){
                buf.append(line+"\n");
            }

            while((line = reader2.readLine())!= null){
                buf.append(line + "\n");
            }
            System.out.println(buf.toString());
*/
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
