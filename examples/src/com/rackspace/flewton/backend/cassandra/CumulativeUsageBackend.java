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
package com.rackspace.flewton.backend.cassandra;

import com.rackspace.flewton.AbstractRecord;
import com.rackspace.flewton.ConfigError;
import com.rackspace.flewton.Flow;

import static com.rackspace.flewton.util.HostResolver.int2ByteBuffer;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sends aggregated data up to cassandra every second or so.  Flows are condensed and buffered.
 * I attemped to make this class threadsafe.  Only curCounters is written to. Anything in oldCounters is considered
 * read-only.
 **/
public class CumulativeUsageBackend extends UsageBackend {
    private static final Logger logger = LoggerFactory.getLogger(CumulativeUsageBackend.class);
    
    // counters get switched every second. This is the maximum number of old counters that will be batched up to cassandra.
    private static final int MAX_COUNTER_BATCH = 5;
    private static final int COUNTER_SWITCH_THRESHOLD = 1000; // ms.
    
    private long lastSwitch = 0;
    private Map<String, OctetCounter> curCounters = new HashMap<String, OctetCounter>();
    private List<Map<String, OctetCounter>> oldCounters = new ArrayList<Map<String, OctetCounter>>();
    
    public CumulativeUsageBackend(HierarchicalConfiguration config) throws ConfigError {
        super(config);
        new Pusher().start();
    }

    /**
     * consumes the record. doesn't write to cassandra.
     */
    public void write(AbstractRecord record) {  
        for (Flow flow : record.flows) {
            boolean srcInternal = resolver.isInternal(flow.sourceAddr);
            boolean dstInternal = resolver.isInternal(flow.destAddr);
            
            if (srcInternal && dstInternal) {
                getCounter(localColFam, flow.sourceAddr).increment(flow.timestampCalculated, flow.numOctets);
                getCounter(localColFam, flow.destAddr).increment(flow.timestampCalculated, flow.numOctets);  
            } else if (srcInternal)
                getCounter(egressColFam, flow.sourceAddr).increment(flow.timestampCalculated, flow.numOctets);
            else if (dstInternal)
                getCounter(ingressColFam, flow.destAddr).increment(flow.timestampCalculated, flow.numOctets);
        }
        
        maybeSwitchCounters();
    }
    
    // possibly switches out the conter with a fresh one so that the old one can be written.
    private synchronized void maybeSwitchCounters() {
        // just make sure to sync on something different than getCounter().
        synchronized (oldCounters) {
            long now = System.currentTimeMillis();
            if (now - lastSwitch > COUNTER_SWITCH_THRESHOLD) {
                oldCounters.add(curCounters);
                curCounters = new HashMap<String, OctetCounter>();
                lastSwitch = now;
            }
        }
    }
    
    // gets the right current counter to increment.
    private OctetCounter getCounter(String cf, InetAddress addr) {
        // just make sure to sync on something different than maybeSwitch().
        synchronized (curCounters) {
            String key = cf + addr.toString();
            OctetCounter counter = curCounters.get(key);
            if (counter == null) {
                counter = new OctetCounter(cf, addr);
                curCounters.put(key, counter);
            }
            return counter;
        }
    }
    
    /** this is where the octet count accumulates */
    private class OctetCounter {
        private String cf;
        private InetAddress addr;
        private List<Long> stamps = new ArrayList<Long>();
        private Map<Long, Integer> counts = new HashMap<Long, Integer>();
        
        private OctetCounter(String cf, InetAddress addr)
        {
            this.cf = cf;
            this.addr = addr;
        }
        
        synchronized void increment(long stamp, int count) {
            stamp = stamp / COUNTER_SWITCH_THRESHOLD * COUNTER_SWITCH_THRESHOLD; // todo: a cheaper way to do this? maybe stamp -= stamp % COUNTER_SWITCH_THRESHOLD
            if (stamps.contains(stamp))
                counts.put(stamp, counts.get(stamp) + count);
            else {
                stamps.add(stamp);
                counts.put(stamp, count);
            }
        }      
    }

    /** 
     * consumes the older counters, pushing them to the cassandra. I don't worry about concurrency here, since anything
     * in oldCounters is done being modified.
     */
    private class Pusher extends Thread {
        public void run() {
            while (true) {
                // wait if there is nothing to write.
                while (oldCounters.size() == 0) {
                    try { sleep(500); } catch (InterruptedException ex) { }
                }
                
                Map<ByteBuffer, Map<String, List<Mutation>>> mutations = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
                // grab everything (at most 5 old counters)
                int batchSize = 0;
                while (oldCounters.size() > 0 && batchSize++ < MAX_COUNTER_BATCH) {
                    Collection<OctetCounter> counters = oldCounters.remove(0).values();
                    for (OctetCounter counter : counters) {
                        for (Map.Entry<Long, Integer> entry : counter.counts.entrySet()) {
                            Mutation m = new Mutation();
                            m.column_or_supercolumn = new ColumnOrSuperColumn();
                            m.column_or_supercolumn.column = new Column(getTimeUUIDByteBuffer(entry.getKey()),
                                    int2ByteBuffer(entry.getValue()), System.currentTimeMillis());
                            m.column_or_supercolumn.column.ttl = colTTL;
                            mutationsForKeyAndCf(ByteBuffer.wrap(counter.addr.getAddress()), counter.cf, mutations).add(m);
                        }
                    }
                }
                
                // now write them.
                long retry = 0; // indicates how many MS to sleep before retrying.
                // first try.
                try {
                    write(mutations);
                } catch (TimedOutException ex) {
                    retry = 1;
                } catch (UnavailableException ex) {
                    retry = 1000;
                } catch (InvalidRequestException e) {
                    logger.error("DROPPING DATA " + e.getMessage(), e);
                } catch (TException e) {
                    logger.error("DROPPING DATA " + e.getMessage(), e);
                }
                
                // maybe second try.
                if (retry > 0) {
                    try { Thread.sleep(retry); } catch (InterruptedException ignore) { }
                    try {
                        write(mutations);
                    } catch (TimedOutException ex) {
                        logger.error("DROPPING DATA " + ex.getMessage(), ex);
                    } catch (UnavailableException ex) {
                        logger.error("DROPPING DATA " + ex.getMessage(), ex);
                    } catch (InvalidRequestException ex) {
                        logger.error("DROPPING DATA " + ex.getMessage(), ex);
                    } catch (TException ex) {
                        logger.error("DROPPING DATA " + ex.getMessage(), ex);
                    }
                }
            }
        }
    }

}
