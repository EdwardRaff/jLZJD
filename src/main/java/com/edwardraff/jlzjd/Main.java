package com.edwardraff.jlzjd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.FileConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import jsat.utils.SystemInfo;


/**
 *
 * @author Edward Raff <Raff.Edward@gmail.com>
 */
public class Main
{
    @Parameter(names = { "-t", "--threshold" }, description = "only show results >=threshold")
    public Integer threshold = 20;
    
    @Parameter(names = { "-r", "--deep" }, description = "generate SDBFs from directories and files")
    private boolean goDeep = false;
    
    @Parameter(names = { "-g", "--gen-compare" }, description = "generate SDBFs and compare all pairs")
    private boolean genCompare = false;
    
    @Parameter(names = { "-p", "--threads" }, description = "compute threads to use")
    public Integer threads = SystemInfo.LogicalCores;
    
    public Integer minhashSize = 1024;
    
    /**
     * This will hold the indexes we are supposed to compare
     */
    @Parameter(names = { "-c", "--compare" }, description = "compare all pairs in SDBF file, or compare two SDBF files to each other", converter = FileConverter.class)
    private List<File> toCompare = new ArrayList<>();
    
    /**
     * This will hold default inputs we should parse
     */
    @Parameter(converter = FileConverter.class)
    private List<File> parameters = new ArrayList<>();
    
    
    //Tools used for parallel execution
    private static ExecutorService ex;
    /**
     * Multiplier applied to the number of problem sub-components created. Done
     * so that work-stealing pool can do load balancing for us
     */
    private static final int P_MUL = 500;
    private static final ReentrantLock stdOutLock = new ReentrantLock();
    /**
     * This list provides a place for ALL StringBuilders used by any and all
     * threads.
     */
    private static final ConcurrentLinkedQueue<StringBuilder> localOutPuts = new ConcurrentLinkedQueue<>();
    /**
     * Access to thread local string builder to place text you want to put to
     * STD out, but can't because its under contention
     */
    private static final ThreadLocal<StringBuilder> localToStdOut = ThreadLocal.withInitial(() -> 
    {
        StringBuilder sb = new StringBuilder();
        localOutPuts.add(sb);
        return sb;
    });
    
    
    public static void main(String... args) throws IOException, InterruptedException
    {
        new Main().run(args);
    }
    
    public void run(String... args) throws IOException, InterruptedException
    {
        JCommander jc = new JCommander(this);
        jc.parse(args);
        ex = Executors.newWorkStealingPool(Math.max(1, threads));
        
        //collect all the files we will be hashing
        List<File> toHash = new ArrayList<>();
        for(File candidate : parameters)
            if(candidate.isFile())
                toHash.add(candidate);
            else if(candidate.isDirectory() && goDeep)
                Files.walk(candidate.toPath()).filter(Files::isRegularFile).forEach(c -> toHash.add(c.toFile()));
        
        if(!toCompare.isEmpty())
        {
            if(toCompare.size() > 2)
                throw new IllegalArgumentException("Can only compare at most two indexes at a time!");
            
            List<int[]> hashesA = new ArrayList<>();
            List<String> filesA = new ArrayList<>();
            
            List<int[]> hashesB = new ArrayList<>();
            List<String> filesB = new ArrayList<>();
            
            readHashesFromFile(toCompare.get(0), hashesA, filesA);
            if(toCompare.size() == 2)
                readHashesFromFile(toCompare.get(1), hashesA, filesA);
            else
            {
                hashesB = hashesA;
                filesB = filesA;
            }
            
            compare(hashesA, filesA, hashesB, filesB);
        }
        else if(genCompare)
            genComp(toHash);
        else
            hashFiles(toHash);
        
        ex.shutdownNow();
    }

    /**
     * Perform comparisons of the given digests lists. If each list points to
     * the same object, only the above-diagonal elements of the comparison
     * matrix will be performed
     *
     * @param hashesA the list of min-hashes for the first set, ordered
     * @param filesA the list of file names for the first set, ordered
     * @param hashesB the list of min-hashes for the second set, ordered
     * @param filesB the list of file names for the first set, ordered
     * @throws InterruptedException 
     */
    public void compare(List<int[]> hashesA, List<String> filesA, List<int[]> hashesB, List<String> filesB) throws InterruptedException
    {
        CountDownLatch latch = new CountDownLatch(hashesA.size());
        for(int i = 0; i < hashesA.size(); i++)
        {
            int[] hAiH = hashesA.get(i);
            String hAiN = filesA.get(i);
            int j_start;
            if(hashesA == hashesB)
                j_start = i+1;//don't self compare / repeat comparisons
            else
                 j_start = 0;
            
            ex.submit(() ->
            {
                for(int j = j_start; j < hashesB.size(); j++)
                {
                    int sim = (int) Math.round(100*LZJDf.similarity(hAiH, hashesB.get(j)));
                    if(sim >= threshold)
                    {
                        StringBuilder toPrint = localToStdOut.get();
                        toPrint.append(String.format(hAiN + "|" + filesB.get(j) + "|%03d\n", sim));
                        
                        tryPrint(toPrint);
                    }
                }
                latch.countDown();
            });
        }
        
        latch.await();
        printAllLocalBuffers();
    }

    private void readHashesFromFile(File f, List<int[]> hashesA, List<String> files) throws IOException
    {
        try(BufferedReader br = new BufferedReader(new FileReader(f)))
        {
            String line;
            while((line = br.readLine()) != null)
            {
                line = line.trim();
                if(line.isEmpty())
                    continue;
                int colonIndx = line.lastIndexOf(":");
                String name = line.substring("lzjd:".length(), colonIndx);
                String b64 = line.substring(colonIndx+1);
                
                byte[] cmp = Base64.getDecoder().decode(b64);
                int hashLen = cmp.length/Integer.SIZE;
                int[] hash = new int[hashLen];
                IntBuffer readBack = ByteBuffer.wrap(cmp).asIntBuffer();
                for(int i = 0; i < hash.length; i++)
                    hash[i] = readBack.get(i);
                
                hashesA.add(hash);
                files.add(name);
            }
        }
    }

    /**
     * Digest and print out the hashes for the given list of files
     * @param toHash the list of files to digest
     * @throws IOException
     * @throws InterruptedException 
     */
    private void hashFiles(List<File> toHash) throws IOException, InterruptedException
    {
        ThreadLocal<byte[]> localTmpSpace = ThreadLocal.withInitial(()->new byte[minhashSize*Integer.BYTES]);
        CountDownLatch latch = new CountDownLatch(SystemInfo.LogicalCores*P_MUL);
        IntStream.range(0, SystemInfo.LogicalCores * P_MUL).forEach(id-> 
        {
            ex.submit(() ->
            {
                for (int i = id; i < toHash.size(); i += SystemInfo.LogicalCores * P_MUL)
                {
                    try
                    {
                        File f = toHash.get(i);
                        LZJDf.min_hashes.clear();//hacky, but I don't really care
                        int[] hash = LZJDf.getMinHash(-1, f, (int) Math.min(minhashSize, f.length()/2));

                        byte[] byte_tmp_space = localTmpSpace.get();
                        ByteBuffer byteBuffer = ByteBuffer.wrap(byte_tmp_space);
                        IntBuffer intBuffer = byteBuffer.asIntBuffer();
                        intBuffer.put(hash);

                        String b64 = Base64.getEncoder().encodeToString(byte_tmp_space);
                        
                        StringBuilder toPrint = localToStdOut.get();
                        toPrint.append("lzjd:").append(f.toString()).append(":").append(b64).append("\n");
                        
                        tryPrint(toPrint);
                    }
                    catch (IOException ex1)
                    {
                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex1);
                    }
                }
                latch.countDown();
            });
        });
        
        latch.await();
        printAllLocalBuffers();
    }

    /**
     * Go through every local string buffer and pring them all to std out. 
     */
    public void printAllLocalBuffers()
    {
        for(StringBuilder sb: localOutPuts)//make sure we print off everything!
        {
            System.out.print(sb.toString());
            sb.setLength(0);
        }
    }

    /**
     * This will attempt to print out the locally built buffer of lines to send
     * to STD out. Will update toPrint as appropriate if the attempt was
     * successful
     *
     * @param toPrint the string buffer to print out
     */
    public void tryPrint(StringBuilder toPrint)
    {
        if(stdOutLock.tryLock())
        {
            System.out.print(toPrint.toString());
            toPrint.setLength(0);
            stdOutLock.unlock();
        }
        else if(toPrint.length() > 1024*1024*10)//you have 10MB worth of ASCII? Just print already!
        {
            stdOutLock.lock();
            try
            {
                System.out.print(toPrint.toString());
                toPrint.setLength(0);
            }
            finally
            {
                stdOutLock.unlock();
            }
        }
    }
    
    /**
     * Generate the set of digests and do the all pairs comparison at the same
     * time.
     *
     * @param toHash the list of files to digest and compare
     * @throws IOException
     * @throws InterruptedException 
     */
    private void genComp(List<File> toHash) throws IOException, InterruptedException
    {
        int[][] hashes = new int[toHash.size()][minhashSize];
        String[] names = new String[toHash.size()];

        CountDownLatch latch = new CountDownLatch(SystemInfo.LogicalCores * P_MUL);
        IntStream.range(0, SystemInfo.LogicalCores * P_MUL).forEach(id-> 
        {
            ex.submit(() ->
            {
                for (int i = id; i < toHash.size(); i += SystemInfo.LogicalCores * P_MUL)
                {
                    try
                    {
                        File f = toHash.get(i);
                        LZJDf.min_hashes.clear();
                        int[] hash = LZJDf.getMinHash(-1, f, minhashSize);
                        hashes[i] = hash;
                        names[i] = f.toString();
                    }
                    catch (IOException ex1)
                    {
                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex1);
                    }
                }
                latch.countDown();
            });
        });

        
        latch.await();
        
        List<int[]> hashList = Arrays.asList(hashes);
        List<String> nameList = Arrays.asList(names);
        
        compare(hashList, nameList, hashList, nameList);
    }
}
