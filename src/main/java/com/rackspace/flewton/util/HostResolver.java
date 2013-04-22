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

import org.apache.commons.configuration.HierarchicalConfiguration;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** determines if a host is in a list of netblocks. */
public class HostResolver {
    private final List<Block> internalBlocks = new ArrayList<Block>(); 
    
    public HostResolver(HierarchicalConfiguration config) {
        for (String desc : config.getStringArray("network"))
            internalBlocks.add(new Block(desc));
    }
    
    /** return true if addr is internal to one of my networks. */
    public boolean isInternal(InetAddress addr) {
        // check all configured netblocks.
        for (Block block : internalBlocks)
            if (block.includes(addr))
                return true;
        return false;
    }
    
    /** netblock abstraction. */
    private static class Block {
        private final int network;
        private final int mask;
        
        /** specify your netblock like "xx.xx.xx.xx/x" */
        private Block(String desc) {
            int slash = desc.indexOf('/');
            assert slash > -1;
            int prefix = Integer.parseInt(desc.substring(slash + 1));
            String[] soctets = desc.substring(0, slash).split("\\.");
            // turn the addr part into an addr.
            int addr = (((byte)Integer.parseInt(soctets[3]) & 0xff) << 0) +
                      (((byte)Integer.parseInt(soctets[2]) & 0xff) << 8) +
                      (((byte)Integer.parseInt(soctets[1]) & 0xff) << 16) +
                      (((byte)Integer.parseInt(soctets[0]) & 0xff) << 24);
            
            // now slam off the bits we don't need.
            mask = 0xffffffff << (32 - prefix);
            network = addr & mask;
        }
        
        /** return true if this block includes an addr. */
        private boolean includes(InetAddress addr) {
            byte[] b = addr.getAddress();
            int address = ((b[3] & 0xFF) << 0) +
                       ((b[2] & 0xFF) << 8) +
                       ((b[1] & 0xFF) << 16) +
                       ((b[0]) << 24);
            return (address & mask) == network;
        }
    }
    
    // serialize an integer. 
    public static byte[] int2byte(int i) {
        byte[] b = new byte[4];
        b[0] = (byte)(i >>> 24);
        b[1] = (byte)(i >>> 16);
        b[2] = (byte)(i >>> 8);
        b[3] = (byte)(i >>> 0);
        return b;
    }
    
    public static ByteBuffer int2ByteBuffer(int i) {
        return ByteBuffer.wrap(int2byte(i));
    }
    
    // serialize a long.
    public static byte[] long2byte(long l) {
        byte[] b = new byte[8];
        b[0] = (byte)(l >>> 56);
        b[1] = (byte)(l >>> 48);
        b[2] = (byte)(l >>> 40);
        b[3] = (byte)(l >>> 32);
        b[4] = (byte)(l >>> 24);
        b[5] = (byte)(l >>> 16);
        b[6] = (byte)(l >>> 8);
        b[7] = (byte)(l >>> 0);
        return b;
    }
}

    
