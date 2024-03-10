package org.ncl.kadrtt.core.cmds;


import io.libp2p.core.PeerId;
import org.ncl.kadrtt.core.Kad;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * IPFSの起動・停止・メッセージ通信を管理するためのスレッドクラスです．
 */
public class IpfsThread implements Runnable{

    private boolean isStarted;

    private boolean isStartCompleted;

    public IpfsThread() {
        this.isStarted = false;
        this.isStartCompleted = false;
    }

    @Override
    public void run() {
        //起動時刻を乱数で決める．
        long startTime = (long)(Math.random() * 5000);
        long start = System.currentTimeMillis();
        double t = 1;

        //要求発生確率の設定
        Kad.lambda = Kad.genDouble2(Kad.lambda_min, Kad.lambda_max, 0.5);
        boolean isGWConfirmed = false;
        while(true){
            try{
                Thread.sleep(100);
                if(isStarted){
                    if(!this.isStartCompleted){
                        continue;
                    }
                    if(!isGWConfirmed){
                        //PeerIDを取得する．
                        String peerid = this.exec("curl -X POST \"http://127.0.0.1:5001/api/v0/dht/getpeerid\"");
                        String gwid = Kad.getIns().getEndPointPeerId();

                        //もしendpointであれば，リストを見てssh接続を行う．
                        if(peerid.equals(gwid)){


                        }
                        isGWConfirmed = true;
                    }

                    //起動しているときの処理
                    double comulative_p = 0.0d;
                    while(true){
                        //指数分布による，累積の確率密度を算出．
                        comulative_p = 1.0d- Math.pow(Math.E, (-1)*t*Kad.lambda);
                        //1秒だけ待つ．
                        // System.out.println("NodeID:"+this.nodeID);
                        Thread.sleep(1000);
                        double randomValue = Math.random();
                        //Interestパケット送信可能状態となった
                        if(randomValue <= comulative_p){
                            break;
                        }
                        t++;
                    }
                    //次は，GET/PUTを決める．
                    //cidの取得，データの取得を事前に行う必要がある．


                    //Thread.sleep(10000);
                    System.out.println("***IPFS Exiting...*****");
                    this.exec("curl -X POST \"http://127.0.0.1:5001/api/v0/exit\"");
                    this.isStarted = false;
                    this.isStartCompleted = false;
                    break;

                    //停止させる．


                }else{
                    //起動していないときの処理
                    Thread.sleep(startTime);
                    //IPFSを起動する．
                    IpfsStarter starter = new IpfsStarter(this);
                    Thread thread = new Thread(starter);
                    thread.start();



                    this.isStarted = true;
                }

            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }

    public void putProcess(){
        
    }

    public String exec(String cmd){
        try{
            List<String> cmdList = new LinkedList<String>();

            StringTokenizer token = new StringTokenizer(cmd, " ");
            while(token.hasMoreTokens()){
                String param = token.nextToken();
                cmdList.add(param);
            }

            ProcessBuilder b = new ProcessBuilder(cmdList);
            Process p = b.start();
            //プロセスが終了するのを待つ．

            p.waitFor();
            int code = p.exitValue();
            System.out.println(code);
            //出力結果を得る．
            BufferedReader reader = new BufferedReader(
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
                buf.append(line);
            }

            while((line = reader2.readLine())!= null){
                //buf.append(line);
            }
            //System.out.println(buf.toString());
            return buf.toString();
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;


    }


    public boolean isStartCompleted() {
        return isStartCompleted;
    }

    public void setStartCompleted(boolean startCompleted) {
        isStartCompleted = startCompleted;
    }
}
