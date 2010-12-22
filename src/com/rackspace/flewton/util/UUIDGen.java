/*
 * Copyright (c) 2010 Rackspace
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
package com.rackspace.flewton.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * The goods are here: www.ietf.org/rfc/rfc4122.txt.
 */
public class UUIDGen
{
    // A grand day! millis at 00:00:00.000 15 Oct 1582.
    private static final long START_EPOCH = -12219292800000L;
    private static final long CLOCK = new Random(System.currentTimeMillis()).nextLong();
    private static int clockOffsetTicker = 0;
    
    // placement of this singleton is important.  It needs to be instantiated *AFTER* the other statics.
    private static final UUIDGen instance = new UUIDGen();
    
    private final long clock;
    private long lastNanos;
    
    private UUIDGen() {
        // make sure someone didn't whack the clock by changing the order of instantiation.
        if (CLOCK == 0) throw new RuntimeException("singleton instantiation is misplaced.");
        
        clock = CLOCK + clockOffsetTicker++;
    }
    
    /**
     * Converts a milliseconds-since-epoch timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     * 
     * @param timeMillis
     * @return a type 1 UUID represented as a byte[]
     */
    public static byte[] getTimeUUIDBytes(long timeMillis) {
        long msb = instance.createTime(timeMillis), lsb = instance.getClockSeqAndNode();
        byte[] uuidBytes = new byte[16];
        
        for (int i = 0; i < 8; i++) {
            uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++) {
            uuidBytes[i] = (byte) (lsb >>> 8 * (7 - i));
        }
        return uuidBytes;
    }
    
    // todo: could cache value if we assume node doesn't change.
    private synchronized long getClockSeqAndNode() {
        long lsb = 0;
        lsb |= (clock & 0x3f00000000000000L) >>> 56; // was 58?
        lsb |= 0x0000000000000080;
        lsb |= (clock & 0x00ff000000000000L) >>> 48; 
        lsb |= makeNode();
        return lsb;
    }
    
    // needs to return two different values for the same when.
    // we can generate at most 10k UUIDs per ms.
    private synchronized long createTime(long when) {
        long nanosSince = (when - START_EPOCH) * 10000;
        if (nanosSince > lastNanos)
            lastNanos = nanosSince;
        else
            nanosSince = ++lastNanos;
        
        long msb = 0L; 
        msb |= (0x00000000ffffffffL & nanosSince) << 32;
        msb |= (0x0000ffff00000000L & nanosSince) >>> 16; 
        msb |= (0xffff000000000000L & nanosSince) >>> 48;
        msb |= 0x0000000000001000L; // sets the version to 1.
        return msb;
    }
    
    // todo: could exploit caching.
    private static long makeNode() {
        // ideally, we'd use the MAC address, but java doesn't expose that.
        try {
            MessageDigest hasher = MessageDigest.getInstance("MD5");
            hasher.digest(InetAddress.getLocalHost().toString().getBytes());
            byte[] hash = hasher.digest();
            long node = 0;
            for (int i = 0; i < Math.min(6, hash.length); i++)
                node |= (0x00000000000000ff & (long)hash[i]) << (5-i)*8;
            assert (0xff00000000000000L & node) == 0;
            return node;
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }
}

// for the curious, here is how I generated START_EPOCH
//        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("GMT-0"));
//        c.set(Calendar.YEAR, 1582);
//        c.set(Calendar.MONTH, Calendar.OCTOBER);
//        c.set(Calendar.DAY_OF_MONTH, 15);
//        c.set(Calendar.HOUR_OF_DAY, 0);
//        c.set(Calendar.MINUTE, 0);
//        c.set(Calendar.SECOND, 0);
//        c.set(Calendar.MILLISECOND, 0);
//        long START_EPOCH = c.getTimeInMillis();