
package com.edwardraff.jlzjd;

import java.io.Serializable;
import java.util.*;
import static jsat.utils.ClosedHashingUtil.*;

/**
 * A utility class for efficiently storing a set of integers. The implementation
 * is based on Algorithm D (Open addressing with double hashing) from Knuth's
 * TAOCP page 528.
 *
 * @author Edward Raff
 */
public class IntSetNoRemove extends AbstractSet<Integer> implements Serializable
{

    private static final long serialVersionUID = -2175363824037596497L;

    private static final int DEFAULT_CAPACITY = 8;

    private float loadFactor;

    private int used = 0;
    /**
     * true if occupied, false otherwise (i.e., free)
     */
    private boolean[] status;
    private int[] keys;
    private final static boolean FREE = false;
    private final static boolean OCCUPIED = true;
    
    /**
     * Creates a new empty integer set
     */
    public IntSetNoRemove()
    {
        this(DEFAULT_CAPACITY);
    }
    
    /**
     * Creates an empty integer set pre-allocated to store a specific number of
     * items
     *
     * @param capacity the number of items to store
     */
    public IntSetNoRemove(int capacity)
    {
        this(capacity, 0.75f);
    }

    /**
     * Creates an empty integer set pre-allocated to store a specific number of
     * items
     *
     * @param capacity the number of items to store
     * @param loadFactor the maximum ratio of used to un-used storage
     */
    public IntSetNoRemove(int capacity, float loadFactor)
    {
        this.loadFactor = loadFactor;
        int size = getNextPow2TwinPrime((int) Math.max(capacity / loadFactor, 4));
        status = new boolean[size];
        keys = new int[size];
        used = 0;
    }
    
    /**
     * Creates a new set of integers from the given set 
     * @param set the set of integers to create a copy of
     */
    public IntSetNoRemove(Set<Integer> set)
    {
        this(set.size());
        for(Integer integer : set)
            this.add(integer);
    }
    
    /**
     * Creates a set of integers from the given collection
     * @param collection a collection of integers to create a set from
     */
    public IntSetNoRemove(Collection<Integer> collection)
    {
        this();
        for(Integer integer : collection)
            this.add(integer);
    }


    /**
     * Gets the index of the given key. Based on that {@link #status} variable,
     * the index is either the location to insert OR the location of the key.
     * 
     * This method returns 2 integer table in the long. The lower 32 bits are
     * the index that either contains the key, or is the first empty index. 
     * 
     * The upper 32 bits is the index of the first position marked as
     * {@link #DELETED} either {@link Integer#MIN_VALUE} if no position was
     * marked as DELETED while searching.
     *
     * @param key they key to search for
     * @return the mixed long containing the index of the first DELETED position
     * and the position that the key is in or the first EMPTY position found
     */
    private int getIndex(int key)
    {
        //D1 
        final int hash = key & 0x7fffffff;
        int i = hash % keys.length;
        
        //D2
        if(status[i] == FREE || keys[i] == key)
            return i;
        
        //D3
        final int c = 1 + (hash % (keys.length -2));
        
        while(true)//this loop will terminate
        {
            //D4
            i -= c;
            if(i < 0)
                i += keys.length;
            //D5
            if( status[i] == FREE || keys[i] == key)
                return i;
        }
    }
    
    private void enlargeIfNeeded()
    {
        if(used < keys.length*loadFactor)
            return;
        //enlarge
        final boolean[] oldSatus = status;
        final int[] oldKeys = keys;
        
        int newSize = getNextPow2TwinPrime(status.length*3/2);//it will actually end up doubling in size since we have twin primes spaced that was
        status = new boolean[newSize];
        keys = new int[newSize];
        
        used = 0;
        for(int oldIndex = 0; oldIndex < oldSatus.length; oldIndex++)
            if(oldSatus[oldIndex] == OCCUPIED)
                add(oldKeys[oldIndex]);
    }
    
    @Override
    public void clear()
    {
        used = 0;
        Arrays.fill(status, FREE);
    }
    
    @Override
    public boolean add(Integer e)
    {
        if(e == null)
            return false;
        return add(e.intValue());
    }
    
    /**
     * 
     * @param e element to be added to this set 
     * @return true if this set did not already contain the specified element 
     */
    public boolean add(int e)
    {
        final int key = e;
        int pair_index = getIndex(key);
//        int deletedIndex = (int) (pair_index >>> 32);
//        int valOrFreeIndex = (int) (pair_index & INT_MASK);
        int valOrFreeIndex = pair_index;
        
        if(status[valOrFreeIndex] == OCCUPIED)//easy case
        {
            return false;//we already had this item in the set!
        }
        //else, not present
        int i = valOrFreeIndex;
//        if(deletedIndex >= 0)//use occupied spot instead
//            i = deletedIndex;
        
        status[i] = OCCUPIED;
        keys[i] = key;
        used++;
        
        enlargeIfNeeded();
        
        return true;//item was not in the set previously
    }

    public boolean contains(int o)
    {
        int index = getIndex(o);
        return status[index] == OCCUPIED;//would be FREE if we didn't have the key
    }

    
    @Override
    public boolean contains(Object o)
    {
        if(o != null && o instanceof Integer)
            return contains(((Integer)o).intValue());
        else
            return false;
    }
    
    
    @Override
    public Iterator<Integer> iterator()
    {
        //find the first starting inded
        int START = 0;
        while (START < status.length && status[START] != OCCUPIED)
            START++;
        if (START == status.length)
            return Collections.emptyIterator();
        final int startPos = START;

        return new Iterator<Integer>()
        {
            int pos = startPos;
            int prevPos = -1;

            @Override
            public boolean hasNext()
            {
                return pos < status.length;
            }

            @Override
            public Integer next()
            {
                //final int make so that object remains good after we call next again
                final int oldPos = prevPos = pos++;
                //find next
                while (pos < status.length && status[pos] != OCCUPIED)
                    pos++;
                //and return new object
                return keys[oldPos];
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public int size()
    {
        return used;
    }
    
}
