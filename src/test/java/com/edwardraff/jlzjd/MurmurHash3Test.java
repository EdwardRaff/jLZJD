/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.edwardraff.jlzjd;

import java.util.Random;
import jsat.utils.random.XORWOW;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Edward Raff
 */
public class MurmurHash3Test
{
    
    public MurmurHash3Test()
    {
    }
    
    @BeforeClass
    public static void setUpClass()
    {
    }
    
    @AfterClass
    public static void tearDownClass()
    {
    }
    
    @Before
    public void setUp()
    {
    }
    
    @After
    public void tearDown()
    {
    }

    /**
     * Test of pushByte method, of class MurmurHash3.
     */
    @org.junit.Test
    public void testPushByte()
    {
        System.out.println("pushByte");
        Random rand = new XORWOW();
        int seed = rand.nextInt();
        MurmurHash3 instance = new MurmurHash3(seed);
        
        byte[] bytes = new byte[4096];
        rand.nextBytes(bytes);
        
        for(int i = 0; i < bytes.length; i++)
            assertEquals("Failed on " + i, MurmurHash3.murmurhash3_x86_32(bytes, 0, i+1, seed), instance.pushByte(bytes[i]));
    }
    
}
