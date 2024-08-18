package org.ncl.kadrtt.core;

import io.libp2p.core.Host;
import org.ncl.kadrtt.core.chunk.AbstractChunkProvider;

/**
 * バックグラウンド処理のためのスレッドです．
 * チャンクを定期的にputします．
 * 全チャンクは一定確率に基づいてputされる．
 * 人気チャンクは，その確率にプラスαされる．
 */
public class BackgroundThread implements Runnable{

    private AbstractChunkProvider pvovider;

    public BackgroundThread(AbstractChunkProvider pvovider) {
        this.pvovider = pvovider;
    }

    public BackgroundThread() {
    }

    @Override
    public void run() {

        Host us = Kad.getIns().getNode();
        while(true){
            try{
                Thread.sleep(10000);
                Kad.getIns().getKadDHT().bootstrap(us);



            }catch(Exception e){
                e.printStackTrace();
            }
        }
    }
}
