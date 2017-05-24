
package com.edwardraff.jlzjd;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import jsat.classifiers.DataPoint;
import jsat.distributions.kernels.KernelTrick;
import jsat.linear.DenseVector;
import jsat.linear.Vec;
import jsat.linear.distancemetrics.DistanceMetric;
import jsat.parameters.Parameter;
import jsat.utils.IntList;

/**
 *
 * @author Edward Raff
 */
public class LZJDf implements DistanceMetric, KernelTrick
{
    static final ConcurrentMap<File, Integer> files = new ConcurrentHashMap<>();
    static final AtomicInteger file_counter = new AtomicInteger();
    static final ConcurrentMap<Integer, int[]> min_hashes = new ConcurrentHashMap<>();
    static ThreadLocal<HashSet<ByteBuffer>> localSets = ThreadLocal.withInitial(() -> new HashSet<>());
    public static int min_hash_size = 1024;
    
    /**
     * Fills a set of ByteBuffers according the the LZ algorithm. Returns the
     * set as a list of unique integer hashes found.
     *
     * @param x_bytes
     * @param length
     * @param x_set
     * @return
     */
    private static List<Integer> fillByteSetLZasInts(byte[] x_bytes, int length, Set<ByteBuffer> x_set)
    {

        IntList ints = new IntList();
        int last_pos = 0;
        int pos = 1;
        int max_len = 0;
        x_set.clear();
        while(pos < length)
        {
            ByteBuffer sub_seq = ByteBuffer.wrap(x_bytes, last_pos, pos-last_pos);
            if(x_set.add(sub_seq))//true if sub_seq wasn't already in it
            {
                ints.add(MurmurHash3.murmurhash3_x86_32(x_bytes, last_pos, pos-last_pos));
                max_len = Math.max(max_len, pos-last_pos);
                last_pos = pos;
            }
            pos++;
        }
        return ints;
    }
    
    private static List<Integer> fillByteSetLZasInts(byte[] x_bytes, Set<ByteBuffer> x_set)
    {
        return fillByteSetLZasInts(x_bytes, x_bytes.length, x_set);
    }
    
    /**
     * Obtains a min-hash set for the given input file
     * @param indx the unique index assigned for this file
     * @param x_file the file to get the LZJD min-hash of
     * @param min_hash_size the max size for the min-hash
     * @return an int array of the min-hash values in sorted order
     * @throws IOException 
     */
    private static int[] getMinHash(int indx, File x_file, int min_hash_size) throws IOException
    {
        int[] x_minset = min_hashes.get(indx);
        if(x_minset == null)
        {
            byte[] x_bytes = Files.readAllBytes(x_file.toPath());
            
            List<Integer> hashes = fillByteSetLZasInts(x_bytes, localSets.get());
            Collections.sort(hashes);
            
            x_minset = new int[Math.min(min_hash_size, hashes.size())];
            for(int i = 0; i < x_minset.length; i++)
                x_minset[i] = hashes.get(i);
            
            
            min_hashes.putIfAbsent(indx, x_minset);
        }
        
        return x_minset;
    }
    
    /**
     * Obtains a DataPoint object for the given file, which stores a representation that can be used with this distance metric
     * @param f the file to get a min-hash for
     * @return a DataPoint that will correspond to the given file 
     */
    public static DataPoint getDPforFile(File f)
    {
        int id;

        id = file_counter.getAndIncrement();
        files.put(f, id);
       
        DenseVector dv = new DenseVector(1);
        dv.set(0, id);

        try 
        {
            getMinHash(id, f, min_hash_size);
        }
        catch (IOException ex) 
        {
            Logger.getLogger(LZJDf.class.getName()).log(Level.SEVERE, null, ex);
        }

       
        return new DataPoint(dv);
    }
    

    @Override
    public boolean isSymmetric()
    {
        return true;
    }
 
    @Override
    public boolean isSubadditive()
    {
        return true;
    }
 
    @Override
    public boolean isIndiscemible()
    {
        return true;
    }
 
    @Override
    public double metricBound()
    {
        return 1;
    }
 
    @Override
    public boolean supportsAcceleration()
    {
        return false;
    }
 
    @Override
    public List<Double> getAccelerationCache(List<? extends Vec> vecs)
    {
        return null;
    }
 
    @Override
    public List<Double> getAccelerationCache(List<? extends Vec> vecs, ExecutorService threadpool)
    {
        return null;
    }
    
    
    @Override
    public double dist(Vec a, Vec b)
    {
        int x = (int) a.get(0);
        int y = (int) b.get(0);
        
        try
        {
            int[] x_minset = getMinHash(x, null, min_hash_size);
            int[] y_minset = getMinHash(y, null, min_hash_size);

            int same = 0;
            
            int x_pos = 0, y_pos = 0;
            while(x_pos < x_minset.length && y_pos < y_minset.length)
            {
                int x_v = x_minset[x_pos];
                int y_v = y_minset[y_pos];
                if(x_v == y_v)
                {
                    same++;
                    x_pos++;
                    y_pos++;
                }
                else if(x_v < y_v)
                    x_pos++;
                else
                    y_pos++;
            }

            return 1.0 - same / (double) (x_minset.length + y_minset.length - same);

        }
        catch (IOException ex)
        {
            Logger.getLogger(LZJDf.class.getName()).log(Level.SEVERE, null, ex);
        }
       
        return 1;
    }
 
    @Override
    public double dist(int a, int b, List<? extends Vec> vecs, List<Double> cache)
    {
        return dist(vecs.get(a), vecs.get(b));
    }
 
    @Override
    public double dist(int a, Vec b, List<? extends Vec> vecs, List<Double> cache)
    {
        return dist(vecs.get(a), b);
    }
 
    @Override
    public List<Double> getQueryInfo(Vec q)
    {
        return null;
    }
 
    @Override
    public double dist(int a, Vec b, List<Double> qi, List<? extends Vec> vecs, List<Double> cache)
    {
        return dist(vecs.get(a), b);
    }
 
    @Override
    public LZJDf clone()
    {
        return this;
    }

    @Override
    public double eval(Vec a, Vec b) {
        return 1-dist(a, b);
    }

    @Override
    public void addToCache(Vec newVec, List<Double> cache) {
        
    }

    @Override
    public double eval(int a, Vec b, List<Double> qi, List<? extends Vec> vecs, List<Double> cache) {
        return 1-dist(a, b, qi, vecs, cache);
    }

    @Override
    public double eval(int a, int b, List<? extends Vec> trainingSet, List<Double> cache) {
        return 1-dist(a, b, trainingSet, cache);
    }

    @Override
    public double evalSum(List<? extends Vec> finalSet, List<Double> cache, double[] alpha, Vec y, int start, int end) {
        double dot = 0;
        for(int i = 0; i < alpha.length; i++)
            if(alpha[i] != 0)
                dot += alpha[i] * eval(i, y, null, finalSet, cache);
        return dot;
    }

    @Override
    public double evalSum(List<? extends Vec> finalSet, List<Double> cache, double[] alpha, Vec y, List<Double> qi, int start, int end) {
        double dot = 0;
        for(int i = 0; i < alpha.length; i++)
            if(alpha[i] != 0)
                dot += alpha[i] * eval(i, y, qi, finalSet, cache);
        return dot;
    }

    @Override
    public boolean normalized() {
        return true;
    }

    @Override
    public List<Parameter> getParameters() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Parameter getParameter(String paramName) {
        return null;
    }
}
