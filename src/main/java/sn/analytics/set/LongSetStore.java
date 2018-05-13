package sn.analytics.set;

import net.openhft.chronicle.map.ChronicleMap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.util.Random;

/**
 * Store Set of Integers for an Id
 * Created by sumanth on 28/04/18.
 */
public class LongSetStore {
    private static LongSetStore ourInstance = new LongSetStore();

    public static LongSetStore getInstance() {
        return ourInstance;
    }

    private LongSetStore() {
    }

    //1 id has many values mapped, like a multimap
    //set stored as bits
    private ChronicleMap<Long,MutableRoaringBitmap> idSetMap;
    int maxElements = 100_000;

    private boolean isInit = false;
    public synchronized void init(int maxElements,int maxSetElements){
        if (isInit) return;
        this.maxElements = maxElements;
        //200 values per id
        Random rgen = new Random(323222);
        MutableRoaringBitmap sampleBM = new MutableRoaringBitmap();
        for(int i =0;i<maxSetElements;i++){
            sampleBM.add(Math.abs(rgen.nextInt(maxSetElements*100)));
        }

        try {
            idSetMap = ChronicleMap
                    .of(Long.class, MutableRoaringBitmap.class).name("dict-set")
                    .entries(maxElements)
                    .averageValueSize(sampleBM.getSizeInBytes())
                    .create();
            isInit = true;
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





    /*public static void main(String [] args){
        int maxElements = 10_000;
        LongSetStore setStore = LongSetStore.getInstance();


        setStore.init(maxElements,250);


        long [] keyIds =new long[]{1234L,1245L,1246L,2345L,3456L,4567L,4578L,4589L};
        int [] setIds = new int[]{1,3,5,9,19,23,45,1000,12000};

        LongSetStore lss = LongSetStore.getInstance();
        for(long id : keyIds){
            lss.addElements(id,setIds);
        }

        for(long id: keyIds){
            for(int sid: setIds){
                //System.out.println(lss.exists(id,sid));
            }
        }

    }

*/
}
