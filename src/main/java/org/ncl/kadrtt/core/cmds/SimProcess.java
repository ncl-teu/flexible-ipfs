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
import java.util.StringTokenizer;

/**
 *
 */
public class SimProcess {
    public static void main(String[] args) {
        try {

            Kad.getIns().initialize("kadrtt.properties");
            IpfsThread ipfs = new IpfsThread();
            Thread p = new Thread(ipfs);
            p.start();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void exec(String cmd) {
        try {
            List<String> cmdList = new LinkedList<String>();

            StringTokenizer token = new StringTokenizer(cmd, " ");
            while (token.hasMoreTokens()) {
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
            while ((line = reader.readLine()) != null) {
                buf.append(line + "\n");
            }

            while ((line = reader2.readLine()) != null) {
                buf.append(line + "\n");
            }
            System.out.println(buf.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
