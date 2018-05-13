package sn.analytics.set;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shaded.org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Created by sumanth on 28/04/18.
 */
public class SimplePersistentStoreTest {
    static final int maxElements=10_000;
    static final  String filePath = "/tmp/datadump.dat";
    @BeforeClass
    public static void init(){

        SimplePersistentStore.getInstance().init(maxElements,filePath);
    }

    @Test
    public void testAddition() throws Exception {
        SimplePersistentStore sDict = SimplePersistentStore.getInstance();
        Set<String> mapKeys = new HashSet<>();

        //323,333,222323
        HashFunction hf = Hashing.murmur3_128(314_159);
        for(int i =0;i < maxElements;i++){
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

    @AfterClass
    public static void close(){
        try {
            SimplePersistentStore.getInstance().close();
            FileUtils.forceDelete(new File(filePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}