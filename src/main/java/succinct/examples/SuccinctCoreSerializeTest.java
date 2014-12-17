package succinct.examples;

import succinct.SuccinctCore;

import java.io.*;

public class SuccinctCoreSerializeTest {

    /**
     * @param args
     *            the command line arguments
     * @throws java.io.FileNotFoundException
     * @throws ClassNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException,
            IOException, ClassNotFoundException {
        File file = new File(args[0]);
        byte[] fileData = new byte[(int) file.length()];
        DataInputStream dis = new DataInputStream(
                new FileInputStream(file));
        dis.readFully(fileData);
        SuccinctCore succinctBuf = new SuccinctCore(
                (new String(fileData) + (char) 1).getBytes(), 3);

        long sum = 0;
        long size = succinctBuf.getOriginalSize();
        System.out.println("Original size = " + size);
        for (long i = 0; i < size; i++) {
            long psi_val = succinctBuf.lookupNPA(i);
            sum += psi_val;
            sum %= size;
        }

        if (sum != 0) {
            System.out.println("NPA Check Failed!");
        } else {
            System.out.println("NPA Check Passed!");
        }

        for (long i = 0; i < size; i++) {
            long sa_val = succinctBuf.lookupSA(i);
            sum += sa_val;
            sum %= size;
        }

        if (sum != 0) {
            System.out.println("SA Check Failed!");
        } else {
            System.out.println("SA Check Passed!");
        }

        for (long i = 0; i < size; i++) {
            long isa_val = succinctBuf.lookupISA(i);
            sum += isa_val;
            sum %= size;
        }

        if (sum != 0) {
            System.out.println("ISA Check Failed!");
        } else {
            System.out.println("ISA Check Passed!");
        }

        FileOutputStream fout = new FileOutputStream(args[0] + ".succinct");
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        oos.writeObject(succinctBuf);
        oos.close();
    }
}
