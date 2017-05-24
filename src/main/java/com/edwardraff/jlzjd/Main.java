/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import jsat.math.OnLineStatistics;


/**
 *
 * @author Edward Raff <Raff.Edward@gmail.com>
 */
public class Main
{
    @Parameter(names = { "-t", "--threshold" }, description = "only show results >=threshold")
    public Integer threshold = 25;
    
    @Parameter(names = { "-r", "--deep" }, description = "generate SDBFs from directories and files")
    private boolean goDeep = false;
    
    @Parameter(names = { "-g", "--gen-compare" }, description = "generate SDBFs and compare all pairs")
    private boolean genCompare = false;
    
    @Parameter(names = { "-p", "--threads" }, description = "compute threads to use")
    public Integer threads = 1;
    
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
    
    public static void main(String... args) throws IOException
    {
        new Main().run(args);
    }
    
    public void run(String... args) throws IOException
    {
        JCommander jc = new JCommander(this);
        jc.parse(args);
        
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
    }

    public void compare(List<int[]> hashesA, List<String> filesA, List<int[]> hashesB, List<String> filesB)
    {
        for(int i = 0; i < hashesA.size(); i++)
        {
            int[] hAiH = hashesA.get(i);
            String hAiN = filesA.get(i);
            int j_start = 0;
            if(hashesA == hashesB)
                j_start = i+1;//don't self compare / repeat comparisons
            for(int j = j_start; j < hashesB.size(); j++)
            {
                int sim = (int) Math.round(100*LZJD.similarity(hAiH, hashesB.get(j)));
                if(sim >= threshold)
                    System.out.printf(hAiN + "|" + filesB.get(j) + "|%03d\n", sim);
            }
        }
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

    private void hashFiles(List<File> toHash) throws IOException
    {
        byte[] byte_tmp_space = new byte[minhashSize*Integer.BYTES];
        for(File f : toHash)
        {
            LZJD.min_hashes.clear();//hacky, but I don't really care
            int[] hash = LZJD.getMinHash(-1, f, (int) Math.min(minhashSize, f.length()/2));
            
            ByteBuffer byteBuffer = ByteBuffer.wrap(byte_tmp_space);
            IntBuffer intBuffer = byteBuffer.asIntBuffer();
            intBuffer.put(hash);
            
            String b64 = Base64.getEncoder().encodeToString(byte_tmp_space);
            System.out.println("lzjd:" + f.toString() + ":" + b64);
        }
    }
    
    private void genComp(List<File> toHash) throws IOException
    {
        List<int[]> hashes = new ArrayList<>(toHash.size());
        List<String> names = new ArrayList<>();
        
        for(File f : toHash)
        {
            LZJD.min_hashes.clear();//hacky, but I don't really care
            int[] hash = LZJD.getMinHash(-1, f, minhashSize);
            hashes.add(hash);
            names.add(f.toString());
        }
        
        compare(hashes, names, hashes, names);
    }
}
