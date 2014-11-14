package succinct.examples;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import succinct.SuccinctFile;

public class SuccinctBenchmark {
    private ArrayList<String> queries;
    private ArrayList<Long> randoms;
    private SuccinctFile sFile;

    private static final int MEASURECOUNT = 10000;
    private static final int WARMUPCOUNT = 10000;
    private static final int COOLDOWNCOUNT = 10000;

    public SuccinctBenchmark(String inputFile, String queryFile) {
        this.sFile = null;
        try {
            this.sFile = new SuccinctFile(inputFile, 3);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        populateRandoms();
        readQueries(queryFile);
    }

    public SuccinctBenchmark(String inputFile) {
        this(inputFile, "");
    }

    public SuccinctBenchmark(SuccinctFile sFile, String queryFile) {
        this.sFile = sFile;
        populateRandoms();
        readQueries(queryFile);
    }

    public SuccinctBenchmark(SuccinctFile sFile) {
        this(sFile, "");
    }

    private void populateRandoms() {
        Random rng = new Random();
        for (long i = 0; i < MEASURECOUNT + WARMUPCOUNT + COOLDOWNCOUNT; i++) {
            randoms.add((long) (rng.nextDouble() * sFile.getOriginalSize()));
        }
    }

    private void readQueries(String queryPath) {
        if (queryPath == "")
            return;
        File queryFile = new File(queryPath);
        BufferedReader in = null;
        try {
            in = new BufferedReader(new InputStreamReader(new FileInputStream(
                    queryFile)));
            String line;
            while ((line = in.readLine()) != null) {
                String[] lineSplits = line.split("\t");
                queries.add(lineSplits[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public void benchmarkCore(String resPath) {
        benchmarkNPA(resPath + "res_npa");
        benchmarkSA(resPath + "res_sa");
        benchmarkISA(resPath + "res_isa");
    }

    public void benchmarkFile(String resPath) {
        benchmarkExtract(resPath + "res_extract");
        benchmarkCount(resPath + "res_count");
        benchmarkSearch(resPath + "res_search");
    }

    public void benchmarkNPA(String resFile) {

        long sum;
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(resFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Warmup
        sum = 0;
        for (int i = 0; i < WARMUPCOUNT; i++) {
            long val = sFile.lookupNPA(randoms.get(i));
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Warmup phase complete! Checksum = " + sum);

        // Measure
        sum = 0;
        for (int i = WARMUPCOUNT; i < WARMUPCOUNT + MEASURECOUNT; i++) {
            long start = System.nanoTime();
            long val = sFile.lookupNPA(randoms.get(i));
            long tdiff = System.nanoTime() - start;
            try {
                out.write(randoms.get(i) + "\t" + val + "\t" + tdiff + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Measure phase complete! Checksum = " + sum);

        // Cooldown
        sum = 0;
        long totCount = WARMUPCOUNT + MEASURECOUNT + COOLDOWNCOUNT;
        for (int i = WARMUPCOUNT + MEASURECOUNT; i < totCount; i++) {
            long val = sFile.lookupNPA(randoms.get(i));
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Cooldown phase complete! Checksum = " + sum);

    }

    public void benchmarkSA(String resFile) {
        long sum;
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(resFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Warmup
        sum = 0;
        for (int i = 0; i < WARMUPCOUNT; i++) {
            long val = sFile.lookupSA(randoms.get(i));
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Warmup phase complete! Checksum = " + sum);

        // Measure
        sum = 0;
        for (int i = WARMUPCOUNT; i < WARMUPCOUNT + MEASURECOUNT; i++) {
            long start = System.nanoTime();
            long val = sFile.lookupSA(randoms.get(i));
            long tdiff = System.nanoTime() - start;
            try {
                out.write(randoms.get(i) + "\t" + val + "\t" + tdiff + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Measure phase complete! Checksum = " + sum);

        // Cooldown
        sum = 0;
        long totCount = WARMUPCOUNT + MEASURECOUNT + COOLDOWNCOUNT;
        for (int i = WARMUPCOUNT + MEASURECOUNT; i < totCount; i++) {
            long val = sFile.lookupSA(randoms.get(i));
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Cooldown phase complete! Checksum = " + sum);
    }

    public void benchmarkISA(String resFile) {
        long sum;
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(resFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Warmup
        sum = 0;
        for (int i = 0; i < WARMUPCOUNT; i++) {
            long val = sFile.lookupISA(randoms.get(i));
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Warmup phase complete! Checksum = " + sum);

        // Measure
        sum = 0;
        for (int i = WARMUPCOUNT; i < WARMUPCOUNT + MEASURECOUNT; i++) {
            long start = System.nanoTime();
            long val = sFile.lookupISA(randoms.get(i));
            long tdiff = System.nanoTime() - start;
            try {
                out.write(randoms.get(i) + "\t" + val + "\t" + tdiff + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Measure phase complete! Checksum = " + sum);

        // Cooldown
        sum = 0;
        long totCount = WARMUPCOUNT + MEASURECOUNT + COOLDOWNCOUNT;
        for (int i = WARMUPCOUNT + MEASURECOUNT; i < totCount; i++) {
            long val = sFile.lookupISA(randoms.get(i));
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Cooldown phase complete! Checksum = " + sum);
    }

    public void benchmarkCount(String resFile) {
        long sum;
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(resFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Measure
        sum = 0;
        for (int i = 0; i < queries.size(); i++) {
            long start = System.nanoTime();
            long val = sFile.count(queries.get(i));
            long tdiff = System.nanoTime() - start;
            try {
                out.write(val + "\t" + tdiff + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sum += val;
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Measure phase complete! Checksum = " + sum);
    }

    public void benchmarkSearch(String resFile) {
        long sum;
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(resFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Measure
        sum = 0;
        for (int i = 0; i < queries.size(); i++) {
            long start = System.nanoTime();
            List<Long> val = sFile.search(queries.get(i));
            long tdiff = System.nanoTime() - start;
            try {
                out.write(val.size() + "\t" + tdiff + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sum += val.size();
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Measure phase complete! Checksum = " + sum);
    }

    public void benchmarkExtract(String resFile) {
        long sum;
        int len = 1000;
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(resFile)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Warmup
        sum = 0;
        for (int i = 0; i < WARMUPCOUNT; i++) {
            String val = sFile.extract(randoms.get(i).intValue(), len);
            sum += val.length();
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Warmup phase complete! Checksum = " + sum);

        // Measure
        sum = 0;
        for (int i = WARMUPCOUNT; i < WARMUPCOUNT + MEASURECOUNT; i++) {
            long start = System.nanoTime();
            String val = sFile.extract(randoms.get(i).intValue(), len);
            long tdiff = System.nanoTime() - start;
            try {
                out.write(randoms.get(i) + "\t" + val.length() + "\t" + tdiff
                        + "\n");
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            sum += val.length();
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Measure phase complete! Checksum = " + sum);

        // Cooldown
        sum = 0;
        long totCount = WARMUPCOUNT + MEASURECOUNT + COOLDOWNCOUNT;
        for (int i = WARMUPCOUNT + MEASURECOUNT; i < totCount; i++) {
            String val = sFile.extract(randoms.get(i).intValue(), len);
            sum += val.length();
            sum %= sFile.getOriginalSize();
        }
        System.err.println("Cooldown phase complete! Checksum = " + sum);
    }
    
    public static void main(String[] args) {
        SuccinctBenchmark sBench = new SuccinctBenchmark(args[0], args[1]);
        sBench.benchmarkCore(args[0]);
        sBench.benchmarkFile(args[0]);
    }

}
