package sn.analytics.set;

import net.openhft.chronicle.map.ChronicleMap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.File;
import java.util.*;

/**
 *
 * Create a persistent set with
 * Memory mapped ChronicleMap and set as Compressed BitMap(Using Roaring bitmap)
 * Created by sumanth on 11/05/18.
 */
//TODO: support for removal of elements
public class DurableLongSet {

    private ChronicleMap<Long,MutableRoaringBitmap> idSetMap;
    int maxElements = 100_000;


    public DurableLongSet(int maxElements,int maxSetElements,String filePath){

        this.maxElements = maxElements;
        //200 values per id
        Random rgen = new Random(323222);
        MutableRoaringBitmap sampleBM = new MutableRoaringBitmap();
        for(int i =0;i<maxSetElements;i++){
            sampleBM.add(Math.abs(rgen.nextInt(maxSetElements*100)));
        }

        try {
            idSetMap = ChronicleMap
                    .of(Long.class, MutableRoaringBitmap.class).name("persistent-map-set")
                    .entries(maxElements)
                    .averageValueSize(sampleBM.getSizeInBytes())
                    .aligned64BitMemoryOperationsAtomic(true)
                    .createOrRecoverPersistedTo(new File(filePath));
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    idSetMap.close();
                }
            }));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public synchronized void addElement(final long key, final int id){

        if (!idSetMap.containsKey(key)){
            idSetMap.put(key,new MutableRoaringBitmap());

        }

        //always an append ,a little costly ops
        MutableRoaringBitmap idSet = idSetMap.get(key);
        idSet.add(id);
        idSetMap.put(key,idSet);
    }

    public synchronized void addElements(final long key, final int ... ids){
        if(ids==null) return;
        if(ids.length==0)return; //can throw exception

        if (!idSetMap.containsKey(key)){
            idSetMap.put(key,new MutableRoaringBitmap());

        }
        MutableRoaringBitmap idSet = idSetMap.get(key);

        for(int i=0;i<ids.length;i++)
            idSet.add(ids[i]);

        idSetMap.put(key,idSet);

    }

    public boolean exists(final long key,final int id){
        if (!idSetMap.containsKey(key)) return false;
        return idSetMap.get(key).contains(id);

    }

    public int getSetCardinality(final long key){
        if (idSetMap.containsKey(key))
            return idSetMap.get(key).getCardinality();
        return 0;
    }

    public Set<Integer> getSet(final long key){
        if (idSetMap.containsKey(key)) {
            Set<Integer> sdump = new HashSet<>();
            Iterator<Integer> itr = idSetMap.get(key).iterator();
            while(itr.hasNext()){
                sdump.add(itr.next());
            }
            return sdump;
        }
        return Collections.emptySet();
    }




}
