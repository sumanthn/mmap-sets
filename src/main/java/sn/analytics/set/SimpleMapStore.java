package sn.analytics.set;

import net.openhft.chronicle.map.ChronicleMap;

import java.util.UUID;

/**
 * Store billion keys UUIDs
 * Created by sumanth on 28/04/18.
 */
public class SimpleMapStore {
    private static SimpleMapStore ourInstance = new SimpleMapStore();

    public static SimpleMapStore getInstance() {
        return ourInstance;
    }
    private ChronicleMap<String,Long> idMap;
    int maxElements = 100_000;
    private SimpleMapStore() {

    }
    private boolean isInit = false;
    public synchronized void init(int maxElements){
        if (isInit) return;
        this.maxElements = maxElements;
        try {
            idMap = ChronicleMap
                    .of(String.class, Long.class).name("simple-dict")
                    .entries(maxElements)
                    .averageKey(UUID.randomUUID().toString())
                    //.averageValueSize(121210L)
                    .create();
            isInit = true;
        }catch (Exception e){
            e.printStackTrace();
        }


    }

    public void addElement(final String key , final Long val){
        idMap.put(key,val);
    }

    public boolean exists(final String key){
        return idMap.containsKey(key);
    }

    public Long getVal(final String key){
        return idMap.get(key);
    }

    public int size(){
        return idMap.size();
    }

/*
    public static void main(String [] args){
        SimpleMapStore sDict  = SimpleMapStore.getInstance();
        int maxElems = 1_100_000;
        sDict.init(maxElems);

        Set<String> mapKeys = new HashSet<>();

        //323,333,222323
        HashFunction hf = Hashing.murmur3_128(314_159);
        for(int i =0;i < maxElems;i++){
            String rId = UUID.randomUUID().toString();
            long hashedId = hf.newHasher().putString(rId, Charset.defaultCharset()).hash().asLong();
            sDict.addElement(rId,hashedId);
            mapKeys.add(rId);
        }



        System.out.println("generated:"+ mapKeys.size() + " consumed " + SimpleMapStore.getInstance().size());

        mapKeys.forEach(k->{
            long hashedId = hf.newHasher().putString(k, Charset.defaultCharset()).hash().asLong();
            if (hashedId!=sDict.getVal(k)) {
                //System.out.println(sDict.getVal(k));
            }
            else{

            }
               // System.out.println("value mismatch " + hashedId + " " + k);
        });

    }*/




}
