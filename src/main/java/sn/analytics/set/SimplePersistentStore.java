package sn.analytics.set;

import net.openhft.chronicle.map.ChronicleMap;

import java.io.File;
import java.util.UUID;

/**
 *
 * Created by sumanth on 28/04/18.
 */
public class SimplePersistentStore {
    private static SimplePersistentStore ourInstance = new SimplePersistentStore();

    public static SimplePersistentStore getInstance() {
        return ourInstance;
    }

    private SimplePersistentStore() {
    }
    private ChronicleMap<String,Long> pMap;
    int maxElements = 100_000;

    private boolean isInit = false;
    public synchronized void init(int maxElements,final String filePath){
        if (isInit) return;
        this.maxElements = maxElements;
        try {

            System.out.println("store in " + filePath);
            pMap = ChronicleMap
                    .of(String.class, Long.class).name("simple-pstore")
                    .entries(maxElements)
                    .averageKey(UUID.randomUUID().toString())
                    .createOrRecoverPersistedTo(new File(filePath));
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {

                    pMap.close();
                }
            }));
            isInit = true;
        }catch (Exception e){
            e.printStackTrace();
        }


    }

    public void close(){
        pMap.close();
    }

    public void addElement(final String key , final Long val){
        pMap.put(key,val);
    }

    public boolean exists(final String key){
        return pMap.containsKey(key);
    }

    public Long getVal(final String key){
        return pMap.get(key);
    }

    public int size(){
        return pMap.size();
    }

/*

    public static void main(String [] args){
        SimplePersistentStore sDict  = SimplePersistentStore.getInstance();
        int maxElems = 100_000;
        String simpleStoreFile = "/tmp/simple-pstore.dat";
        sDict.init(maxElems,simpleStoreFile);

        Set<String> mapKeys = new HashSet<>();

        //323,333,222323
        HashFunction hf = Hashing.murmur3_128(314_159);
        for(int i =0;i < maxElems;i++){
            String rId = UUID.randomUUID().toString();
            long hashedId = hf.newHasher().putString(rId, Charset.defaultCharset()).hash().asLong();
            sDict.addElement(rId,hashedId);
            mapKeys.add(rId);
        }



        System.out.println("generated:"+ mapKeys.size() + " consumed " + SimplePersistentStore.getInstance().size());

        mapKeys.forEach(k->{
            long hashedId = hf.newHasher().putString(k, Charset.defaultCharset()).hash().asLong();
            if (hashedId!=sDict.getVal(k)) {
                //System.out.println(sDict.getVal(k));
            }
            else{

            }
            // System.out.println("value mismatch " + hashedId + " " + k);
        });

    }
*/




}
