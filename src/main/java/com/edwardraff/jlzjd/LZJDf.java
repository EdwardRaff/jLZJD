package com.edwardraff.jlzjd;


import com.google.common.collect.Ordering;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
    static ThreadLocal<byte[]> localByteBuffer = ThreadLocal.withInitial(() -> new byte[4*1024]);
    public static int min_hash_size = 1024;
    
    static final ThreadLocal<IntList> LOCAL_INT_LIST = ThreadLocal.withInitial(()->new IntList());
    static final ThreadLocal<IntSetNoRemove> LOCAL_X_SET = ThreadLocal.withInitial(()->new IntSetNoRemove(1024, 0.65f));
    
    /**
     * Fills a set of ByteBuffers according the the LZ algorithm. Returns the
     * set as a list of unique integer hashes found.
     *
     * @param ints the location t store all of the integer hashes of the sub-sequences
     * @param is the byte source to hash
     */
    private static void getAllHashes(IntList ints, InputStream is) throws IOException
    {
        IntSetNoRemove x_set = LOCAL_X_SET.get();
        x_set.clear();
        MurmurHash3 running_hash = new MurmurHash3();
        int pos = 0;
        int end = 0;
        byte[] buffer = localByteBuffer.get();

        while(true)
        {
            if(end == pos)//we need more bytes!
            {
                end = is.read(buffer);
                if(end < 0)
                    break;//EOF, we are done
                pos = 0;
            }
            //else, procceed
            int hash = running_hash.pushByte(buffer[pos++]);
            if(x_set.add(hash))
            {//never seen it before, put it in!
                ints.add(hash);
                running_hash.reset();
            }
        }
    }
    
    /**
     * Obtains a min-hash set for the given input file
     * @param indx the unique index assigned for this file
     * @param x_file the file to get the LZJDf min-hash of
     * @param min_hash_size the max size for the min-hash
     * @return an int array of the min-hash values in sorted order
     * @throws IOException 
     */
    protected static int[] getMinHash(int indx, File x_file, int min_hash_size) throws IOException
    {
        int[] x_minset = min_hashes.get(indx);
        if(x_minset == null)
        {
            try(FileInputStream fis = new FileInputStream(x_file))
            {
                IntList hashes = LOCAL_INT_LIST.get();
                getAllHashes(hashes, fis);

                List<Integer> sub_hashes = Ordering.natural().leastOf(hashes, Math.min(min_hash_size, hashes.size()));

                x_minset = new int[sub_hashes.size()];
                for(int i = 0; i < x_minset.length; i++)
                    x_minset[i] = sub_hashes.get(i);
                Arrays.sort(x_minset);

                min_hashes.putIfAbsent(indx, x_minset);
            }
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
    
    /**
     * Obtains a DataPoint object for the given file, which stores a representation that can be used with this distance metric
     * @param f the file to get a min-hash for
     * @return a DataPoint that will correspond to the given file 
     */
    public static int[] getMHforFile(File f)
    {
        int id;

        id = file_counter.getAndIncrement();
        files.put(f, id);
       
        DenseVector dv = new DenseVector(1);
        dv.set(0, id);

        try 
        {
            return getMinHash(id, f, min_hash_size);
        }
        catch (IOException ex) 
        {
            Logger.getLogger(LZJDf.class.getName()).log(Level.SEVERE, null, ex);
        }
        return new int[0];
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

            return dist(x_minset, y_minset);

        }
        catch (IOException ex)
        {
            Logger.getLogger(LZJDf.class.getName()).log(Level.SEVERE, null, ex);
        }
       
        return 1;
    }

    public static double dist(int[] x_minset, int[] y_minset)
    {
        double sim = similarity(x_minset, y_minset);
        return 1.0 - sim;
    }

    public static double similarity(int[] x_minset, int[] y_minset)
    {
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
        double sim = same / (double) (x_minset.length + y_minset.length - same);
        return sim;
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
