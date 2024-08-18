package org.ncl.kadrtt.core.chunk;

import java.util.Iterator;
import java.util.List;

public abstract class AbstractChunkProvider {


    public void putProcess(){
        //CIDリストを取得して，putする．
        List<String> candidates = this.selectCIDList();
        Iterator<String> cIte = candidates.iterator();
        while(cIte.hasNext()){
            String cid = cIte.next();

        }


    }

    /**
     *
     * @return
     */
    public abstract List<String> selectCIDList();


}
