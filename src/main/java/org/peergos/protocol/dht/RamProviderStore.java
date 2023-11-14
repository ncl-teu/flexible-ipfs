package org.peergos.protocol.dht;

import io.ipfs.multihash.*;
import org.ncl.kadrtt.core.Kad;
import org.peergos.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class RamProviderStore implements ProviderStore {

    private Map<Multihash, Set<PeerAddresses>> store = new HashMap<>();

    public void write(){
        try{
            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(Kad.providerPath));
            oos.writeObject(this.store);
            oos.flush();
            oos.close();
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    public void read(){
        try{

            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(Kad.providerPath));
            this.store = (Map<Multihash, Set<PeerAddresses>>)ois.readObject();

        }catch(Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public synchronized void addProvider(Multihash m, PeerAddresses peer) {
        //this.read();
        store.putIfAbsent(m, new HashSet<>());
        store.get(m).add(peer);
        //this.write();

    }

    @Override
    public synchronized Set<PeerAddresses> getProviders(Multihash m) {


        /*Set<PeerAddresses> retSet = new HashSet<PeerAddresses>();
        try{

            this.read();
            retSet = this.store.get(m);
            return retSet;


        }catch(Exception e){
            e.printStackTrace();
        }
        return retSet;*/
        return store.getOrDefault(m, Collections.emptySet());
    }
}
