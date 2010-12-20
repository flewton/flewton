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
import com.rackspace.flewton.backend.AbstractBackend;
import com.rackspace.flewton.util.HostResolver;
import static com.rackspace.flewton.util.HostResolver.int2ByteBuffer;

import static com.eaio.uuid.UUIDGen.createTime;
import static com.eaio.uuid.UUIDGen.getClockSeqAndNode;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UsageBackend extends AbstractBackend {
    protected static final int DEFAULT_TTL = 7 * 24 * 60 * 60;
    private static final Logger logger = LoggerFactory.getLogger(UsageBackend.class);
    
    protected final String localColFam;
    protected final String ingressColFam;
    protected final String egressColFam;
    protected final int colTTL;
    
    protected HostResolver resolver;
    private ThriftClientPool clientPool;

    private String getRequiredString(HierarchicalConfiguration config, String key) throws ConfigError {
        String value = config.getString(key);
        if (value == null)
            throw new ConfigError("missing required config property: " + key);
        return value;
    }

    public UsageBackend(HierarchicalConfiguration config) throws ConfigError {
        super(config);
        
        final String keyspace = getRequiredString(config, "keyspace");
        
        localColFam = getRequiredString(config, "lanCf");
        ingressColFam = getRequiredString(config, "wanInCf");
        egressColFam = getRequiredString(config, "wanOutCf");
        colTTL = config.getInt("columnTTLSecs", DEFAULT_TTL);
        
        resolver = new HostResolver(config);
        clientPool = new ThriftClientPool(keyspace, config.getStringArray("storageNode"));
    }

    /**
     * attempts to write the record.  will retry once if there is a time out or unavailable error. all other errors
     * are treated as unrecoverable and the data is dropped.
     */
    public void write(AbstractRecord record) {
        final long ts = System.currentTimeMillis();
        final Map<ByteBuffer, Map<String, List<Mutation>>> mutations = makeMutations(record, resolver, ts);
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
    
    // convert record to a mapped list of mutations suitable for batch_mutate.
    private Map<ByteBuffer, Map<String, List<Mutation>>> makeMutations(AbstractRecord record, HostResolver resolver, long ts) {
        Map<ByteBuffer, Map<String, List<Mutation>>> mutations = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        for (Flow flow : record.flows) {
            boolean srcInternal = resolver.isInternal(flow.sourceAddr);
            boolean dstInternal = resolver.isInternal(flow.destAddr);
            
            if (srcInternal && dstInternal) {
                // src
                Mutation m = new Mutation();
                m.column_or_supercolumn = new ColumnOrSuperColumn();
                m.column_or_supercolumn.column = new Column(getTimeUUIDByteBuffer(flow.timestampCalculated),
                        int2ByteBuffer(flow.numOctets), ts);
                m.column_or_supercolumn.column.ttl = colTTL;
                mutationsForKeyAndCf(ByteBuffer.wrap(flow.sourceAddr.getAddress()), localColFam, mutations).add(m);
                // dst
                m = new Mutation();
                m.column_or_supercolumn = new ColumnOrSuperColumn();
                m.column_or_supercolumn.column = new Column(getTimeUUIDByteBuffer(flow.timestampCalculated),
                        int2ByteBuffer(flow.numOctets), ts);
                m.column_or_supercolumn.column.ttl = colTTL;
                mutationsForKeyAndCf(ByteBuffer.wrap(flow.destAddr.getAddress()), localColFam, mutations).add(m);
            } else if (srcInternal) { // inbound
                Mutation m = new Mutation();
                m.column_or_supercolumn = new ColumnOrSuperColumn();
                m.column_or_supercolumn.column = new Column(getTimeUUIDByteBuffer(flow.timestampCalculated),
                        int2ByteBuffer(flow.numOctets), ts);
                m.column_or_supercolumn.column.ttl = colTTL;
                mutationsForKeyAndCf(ByteBuffer.wrap(flow.sourceAddr.getAddress()), ingressColFam, mutations).add(m);
            } else if (dstInternal) { // outbound
                Mutation m = new Mutation();
                m.column_or_supercolumn = new ColumnOrSuperColumn();
                m.column_or_supercolumn.column = new Column(getTimeUUIDByteBuffer(flow.timestampCalculated),
                        int2ByteBuffer(flow.numOctets), ts);
                m.column_or_supercolumn.column.ttl = colTTL;
                mutationsForKeyAndCf(ByteBuffer.wrap(flow.destAddr.getAddress()), egressColFam, mutations).add(m);
            } else {
                // FIXME: can actually happen.
                logger.error("{} nor {} belong to us, how can this be?", flow.sourceAddr.getCanonicalHostName(),
                        flow.destAddr.getCanonicalHostName());
                continue;
            }
        }
        return mutations;
    }
    
    protected void write(Map<ByteBuffer, Map<String, List<Mutation>>> mutations)
    throws TimedOutException, UnavailableException, InvalidRequestException, TException {
        Cassandra.Client client = borrowClient();
        boolean reuseClient = true;
        
        try {
            client.batch_mutate(mutations, ConsistencyLevel.ONE);
        } catch (TException texcep) {
            logger.warn("Exception encountered writing to {}",
                        ((TSocket)client.getInputProtocol().getTransport()).getSocket().getInetAddress().getHostAddress());
            reuseClient = false;
            invalidateClient(client);
            throw texcep;
        } finally {
            // Return the client to the pool, unless a TException was thrown.
            if (reuseClient)
                returnClient(client);
        }
    }
    
    // ensures integrity of the map while handling the null cases.
    protected static List<Mutation> mutationsForKeyAndCf(ByteBuffer key, String cfName, Map<ByteBuffer, Map<String, List<Mutation>>> map) {
        Map<String, List<Mutation>> cfMap = map.get(key);
        if (cfMap == null) {
            cfMap = new HashMap<String, List<Mutation>>();
            map.put(key, cfMap);
        }
        List<Mutation> mutations = cfMap.get(cfName);
        if (mutations == null) {
            mutations = new ArrayList<Mutation>();
            cfMap.put(cfName, mutations);
        }
        return mutations;
    }
    
    /**
     * Converts a milliseconds-since-epoch timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     * 
     * @param timeMillis
     * @return a type 1 UUID represented as a byte[]
     */
    protected static byte[] getTimeUUIDBytes(long timeMillis) {
        long msb = createTime(timeMillis), lsb = getClockSeqAndNode();
        byte[] uuidBytes = new byte[16];
        
        for (int i = 0; i < 8; i++) {
            uuidBytes[i] = (byte) (msb >>> 8 * (7 - i));
        }
        for (int i = 8; i < 16; i++) {
            uuidBytes[i] = (byte) (lsb >>> 8 * (7 - i));
        }
        
        return uuidBytes;
    }
    
    /**
     * Converts a milliseconds-since-epoch timestamp into the 16 byte representation
     * of a type 1 UUID (a time-based UUID).
     * 
     * @param timeMillis
     * @return a type 1 UUID represented as a ByteBuffer
     */
    protected static ByteBuffer getTimeUUIDByteBuffer(long timeMillis) {
        return ByteBuffer.wrap(getTimeUUIDBytes(timeMillis));
    }
    
    private Cassandra.Client borrowClient() {
        try {
            return (Cassandra.Client)clientPool.borrowObject();
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }
    
    private void returnClient(Object obj) {
        try {
            clientPool.returnObject(obj);
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }
    
    private void invalidateClient(Object obj) {
        try {
            clientPool.invalidateObject(obj);
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }
}