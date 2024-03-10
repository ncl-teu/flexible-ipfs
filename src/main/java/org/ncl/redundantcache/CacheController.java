package org.ncl.redundantcache;

import java.util.Timer;

public class CacheController implements Runnable{
    @Override
    public void run() {

        Timer timer = new Timer();
        CacheTimer ct = new CacheTimer();
        timer.scheduleAtFixedRate(ct, 10000, 1000);
    }
}
