package sn.analytics.set;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

/**
 * Created by sumanth on 28/04/18.
 */
public class SimpleMapStoreTest {

    static final int maxElements=10_000;
    @BeforeClass
    public static void init(){

        SimpleMapStore.getInstance().init(maxElements);
    }

    @Test
    public void addElement() throws Exception {
        Set<String> refKeys = new HashSet<>();

        //tests all
        //323,333,222323
        HashFunction hf = Hashing.murmur3_128(314_159);
        for(int i =0;i < maxElements;i++){
            String rId = UUID.randomUUID().toString();
            long hashedId = hf.newHasher().putString(rId, Charset.defaultCharset()).hash().asLong();
            SimpleMapStore.getInstance().addElement(rId,hashedId);
            refKeys.add(rId);
        }
        assertTrue(maxElements==SimpleMapStore.getInstance().size());
        refKeys.forEach(k->{
            long hashedId = hf.newHasher().putString(k, Charset.defaultCharset()).hash().asLong();
            assertTrue(hashedId==SimpleMapStore.getInstance().getVal(k));

        });
    }



}