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

package com.rackspace.flewton.backend;

import java.net.InetAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rackspace.flewton.AbstractRecord;
import com.rackspace.flewton.Flow;
import com.rackspace.flewton.util.HostResolver;

public class TopTalkersBackend extends AbstractBackend {
    // LRU Map; expires old entries when new ones are added.
    private class CacheMap<K, V> extends LinkedHashMap<K, V> {
        private static final long serialVersionUID = 1L;
        private final int maxEntries;
        
        private CacheMap(int maxEntries) {
            super((int)(maxEntries*0.75f), 0.75f, true);
            this.maxEntries = maxEntries;
        }
        
        @SuppressWarnings("unchecked")
        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > maxEntries;
        }
    }

    public static final int DEFAULT_INTERVAL_SECS = 60 * 60;
    public static final int DEFAULT_MAX_ENTRIES = 1000;
    private static final Logger logger = LoggerFactory.getLogger(TopTalkersBackend.class);
    
    private final Map<InetAddress, Long> cache;
    private final HostResolver resolver;
    private final int intervalSecs;
    private volatile Long lastStatsDump = System.currentTimeMillis();
    
    public TopTalkersBackend(HierarchicalConfiguration config) {
        super(config);
        
        int maxEntries = config.getInt("maxEntries", DEFAULT_MAX_ENTRIES);
        intervalSecs = config.getInt("intervalSecs", DEFAULT_INTERVAL_SECS);
        
        cache = Collections.synchronizedMap(new TopTalkersBackend.CacheMap<InetAddress, Long>(maxEntries));
        resolver = new HostResolver(config);
    }

    // Stores a new value, or updates to the sum of this and the previous.
    private void storeCache(InetAddress key, Long value) {
        if (!(cache.containsKey(key))) {
            cache.put(key, value);
        } else {
            // Don't add if it would result in an overflow
            if (!(cache.get(key) > (Long.MAX_VALUE - value)))
                cache.put(key, (cache.get(key) + value));
        }
    }
    
    // Drop the statistics map to the logger, and purge.
    private void dumpStatistics() {
        for (Map.Entry<InetAddress, Long> stat : cache.entrySet())
            logger.info("host={}, bytes={}", stat.getKey().getHostAddress(), stat.getValue());
        
        cache.clear();
    }
    
    public void write(AbstractRecord record) {
        boolean srcInternal, dstInternal;
        
        for (Flow flow : record.flows) {
            srcInternal = resolver.isInternal(flow.sourceAddr);
            dstInternal = resolver.isInternal(flow.destAddr);
            
            // Traffic was internal to our network
            if (srcInternal && dstInternal) {
                storeCache(flow.sourceAddr, (long)flow.numOctets);
                storeCache(flow.destAddr, (long)flow.numOctets);
            // Traffic was outgoing
            } else if (srcInternal) {
                storeCache(flow.sourceAddr, (long)flow.numOctets);
            // Traffic was incoming
            } else {
                storeCache(flow.destAddr, (long)flow.numOctets);
            }
        }
        
        long currTime = System.currentTimeMillis();
        boolean dumpStats = false;
        
        if (((currTime - lastStatsDump) / 1000) > intervalSecs) {
            synchronized (this) {
                if (((currTime - lastStatsDump) / 1000) > intervalSecs) {
                    lastStatsDump = currTime;
                    dumpStats = true;
                }   
            }
        }
        
        if (dumpStats)
            dumpStatistics();
    }
}
