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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Client pool and factory classes
public class ThriftClientPool extends GenericObjectPool implements ThriftClientPoolMBean {
    private static final Logger logger = LoggerFactory.getLogger(ThriftClientPool.class);
    
    public ThriftClientPool(String keyspace, String[] hosts) {
        super(new ThriftClientFactory(keyspace, hosts));
        // Keep as many connection instances around as we have nodes.
        setMinIdle(hosts.length);
        
        // Create or destroy instances as needed every 2 seconds.
        setTimeBetweenEvictionRunsMillis(2000);
        
        // LIFO false is equivalent to FIFO true, which is desirable here
        // for load-balancing.
        setLifo(false);
        
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try {
            mbs.registerMBean(this, new ObjectName("com.rackspace.flewton.backend.cassandra:type=ThriftClientPool"));
        } catch (Exception e) {
            logger.error("Unable to register MBean", e);
        }
    }
}

class ThriftClientFactory implements PoolableObjectFactory {
    private static final Logger logger = LoggerFactory.getLogger(ThriftClientFactory.class);
    
    private final String keyspace;
    private Map<String, Integer> hosts = new HashMap<String, Integer>();
    private String[] hostNames;
    private volatile Integer hostsPointer = 0;
    
    public ThriftClientFactory(String keyspace, String[] hosts) {
        this.keyspace = keyspace;
        
        for (String host : hosts) {
            String[] elems = host.split(":");
            this.hosts.put(elems[0], Integer.parseInt(elems[1]));
        }
        
        hostNames = new String[this.hosts.size()];
        hostNames = this.hosts.keySet().toArray(hostNames);
    }
    
    @Override
    public void activateObject(Object arg0) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void destroyObject(Object arg0) throws Exception {
        ((Cassandra.Client)arg0).getInputProtocol().getTransport().close();
    }

    @Override
    public Object makeObject() throws Exception {
        String hostName = hostNames[hostsPointer];
        Integer port = hosts.get(hostName);
        
        // Move through the list of hosts (balance).
        synchronized(hostsPointer) {
            if (hostsPointer < (hostNames.length - 1)) hostsPointer++;
            else hostsPointer = 0;
        }
        
        return connect(hostName, port, this.keyspace);
    }

    @Override
    public void passivateObject(Object arg0) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean validateObject(Object arg0) {
        return ((Cassandra.Client)arg0).getInputProtocol().getTransport().isOpen();
    }
    
    // connects and sets keyspace.
    private static Cassandra.Client connect(String host, int port, String keyspace)
    throws InvalidRequestException, TException
    {
        TSocket socket = new TSocket(host, port);
        TTransport transport = new TFramedTransport(socket);
        TProtocol protocol = new TBinaryProtocol(transport); // no TBinaryProtocolAccelerated in java?
        Cassandra.Client client = new Cassandra.Client(protocol);
        socket.open();
        try {
            client.set_keyspace(keyspace);
        } catch (InvalidRequestException oops) {
            // wax-off
            transport.close();
            throw oops;
        }
        
        logger.info("Connected to Cassandra @ {}:{}", host, port);
        
        return client;
    }
}