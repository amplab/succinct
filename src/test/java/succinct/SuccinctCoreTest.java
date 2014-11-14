package succinct;

import static org.junit.Assert.*;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;

import org.junit.Before;
import org.junit.Test;

public class SuccinctCoreTest {

    private SuccinctCore sCore;
    
    @Before
    public void setUp() throws Exception {
        String fileName = "data/test_file";
        File file = new File(fileName);
        byte[] fileData = new byte[(int) file.length()];
        try (DataInputStream dis = new DataInputStream(
                new FileInputStream(file))) {
            dis.readFully(fileData);
        }
        sCore = new SuccinctCore(
                (new String(fileData) + (char) 1).getBytes(), 3);
    }

    @Test
    public void testLookupNPA() {
        // TODO: Substitute with a more rigorous test
        long sum = 0;
        long size = sCore.getOriginalSize();
        for (long i = 0; i < size; i++) {
            long psi_val = sCore.lookupNPA(i);
            sum += psi_val;
            sum %= size;
        }
        
        assertEquals(0, sum);
    }
    
    @Test
    public void testLookupSA() {
        // TODO: Substitute with a more rigorous test
        long sum = 0;
        long size = sCore.getOriginalSize();
        for (long i = 0; i < size; i++) {
            long psi_val = sCore.lookupSA(i);
            sum += psi_val;
            sum %= size;
        }
        
        assertEquals(0, sum);
    }
    
    @Test
    public void testLookupISA() {
        // TODO: Substitute with a more rigorous test
        long sum = 0;
        long size = sCore.getOriginalSize();
        for (long i = 0; i < size; i++) {
            long psi_val = sCore.lookupISA(i);
            sum += psi_val;
            sum %= size;
        }
        
        assertEquals(0, sum);
    }

}
