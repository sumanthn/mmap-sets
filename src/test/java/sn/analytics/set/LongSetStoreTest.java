package sn.analytics.set;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by sumanth on 11/05/18.
 */
public class LongSetStoreTest {
    static final long [] keyIds =new long[]{1234L,1245L,1246L,2345L,3456L,4567L,4578L,4589L};
    static final int [] setElements = new int[]{1,3,5,9,19,23,45,1000,12000};

    @BeforeClass
    public static void init(){
        int maxElements = 10_000;
        LongSetStore setStore = LongSetStore.getInstance();


        setStore.init(maxElements,250);


        for(long id : keyIds){
            setStore.addElements(id,setElements);
        }
    }


    @Test
    public void addElements() throws Exception {

        for(int i=0;i<keyIds.length;i++) {
            LongSetStore.getInstance().addElements(keyIds[i],setElements);
        }
        for(long id: keyIds){
            for(int sid: setElements){
                //System.out.println(lss.exists(id,sid));
                assertTrue((LongSetStore.getInstance().exists(id, sid)));
            }
        }

    }



}