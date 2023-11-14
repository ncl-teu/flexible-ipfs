package org.peergos.blockstore.filters;

import java.math.BigInteger;  
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;  
import java.security.NoSuchAlgorithmException;  

import java.nio.ByteBuffer;

// Java program to calculate SHA hash value  

public class HashFunctions {  
	private static byte[] getSHA(String input) throws NoSuchAlgorithmException 
	{  
		MessageDigest md = MessageDigest.getInstance("SHA-256");  
		return md.digest(input.getBytes(StandardCharsets.UTF_8));  
	} 

	public static int hash(String key, HashType ht) {
		if (ht == HashType.cryptographic) {
			return cryptographic_hash(key);
		}
		else {
			return normal_hash(Integer.parseInt(key));
		}
	}

	public static int cryptographic_hash(String key) {
		BigInteger number = new BigInteger("0");
		
		try {
			number = new BigInteger(1, HashFunctions.getSHA(key));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		int key_signature = Math.abs(number.intValue());
		return key_signature;
	}
	
	public static int normal_hash(int x) {
	    x = ((x >> 16) ^ x) * 0x45d9f3b;
	    x = ((x >> 16) ^ x) * 0x45d9f3b;
	    x = (x >> 16) ^ x;
	    return x;
	}

	public static long xxhash(ByteBuffer buffer){
		buffer.rewind();
		return XxHash.xxHash64(buffer, 0L);
	}

	public static long xxhash(long input){
		return XxHash.xxHash64(input, 0L);
	}
	
	public static long xxhash(long input, long seed){
		return XxHash.xxHash64(input, seed);
	}

} 

