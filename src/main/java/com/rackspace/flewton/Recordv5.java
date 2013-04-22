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

package com.rackspace.flewton;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Recordv5 extends AbstractRecord {
    private static final Logger logger = LoggerFactory.getLogger(Recordv5.class);
    public static final int HEADER_SIZE = 24;
    public static final int FLOW_SIZE = 48;

    /**
     * Accepts a ChannelBuffer whose readerIndex has already been advanced past the
     * one byte version found at the front of all Netflow headers.
     * 
     * @param buffer datagram message received from ChannelHandler
     * @throws CorruptDatagram if unable to parse a valie v5 header
     */
    public Recordv5(ChannelBuffer buffer) throws CorruptDatagram {
        super(buffer);
        
        ChannelBuffer header = getHeader(buffer);
        int count = header.readUnsignedShort();
        long sys_uptime = header.readUnsignedInt();
        // unix_secs + unix_nsecs (converted to millis)
        long millisSinceEpoch = (readLong(4, header) * 1000) + (readLong(4, header) / 1000000);
  
        for (int i = 1; i <= count; i++) {
            ChannelBuffer flowData;
            try {
                flowData = getFlow(buffer);
            } catch (CorruptDatagram err) {
                logger.error("Datagram truncated at flow {} of {}.", i, count);
                break;
            }
            
            Flow flow = new Flow();
            
            try {
                flow.sourceAddr = InetAddress.getByAddress(flowData.readBytes(4).array());
                flow.destAddr = InetAddress.getByAddress(flowData.readBytes(4).array());
                flow.nextHop = InetAddress.getByAddress(flowData.readBytes(4).array());
            } catch (UnknownHostException e) {
                // This should never happen; 4 bytes will always be legal IPv4.
                throw new RuntimeException("Failed parsing IPv4 address!", e);
            }
            flow.snmpIn = flowData.readUnsignedShort();
            flow.snmpOut = flowData.readUnsignedShort();
            flow.numPackets = flowData.readUnsignedInt();
            flow.numOctets = flowData.readUnsignedInt();
            flow.timeFirst = flowData.readUnsignedInt();
            flow.timeLast = flowData.readUnsignedInt();
            flow.sourcePort = flowData.readUnsignedShort();;
            flow.destPort = flowData.readUnsignedShort();
            flow.tcpFlags = flowData.readByte();
            flow.protocol = flowData.readByte();
            flow.tos = flowData.readByte();
            flow.sourceAS = flowData.readUnsignedShort();
            flow.destAS = flowData.readUnsignedShort();
            flow.timestampCalculated = millisSinceEpoch + (flow.timeLast - sys_uptime);
            
            flows.add(flow);
        }
    }
    
    // reads n bytes and treats it as a long. this basically gives us a wasteful unsigned integer (when reading 4 bytes).
    private static long readLong(int n, ChannelBuffer buff) {
        assert n < 8;
        long l = 0;
        for (int i = 0; i < n; i++)
        {
            l <<= 8;
            l |= (0x00000000000000FF & buff.readByte());
        }
        return l;
    }
    
    // Netflow v5 headers are 24 bytes (22 after the version).
    private static ChannelBuffer getHeader(ChannelBuffer buff) throws CorruptDatagram {
        if (buff.readableBytes() < (HEADER_SIZE-2))
            throw new CorruptDatagram("Insufficent data to parse Netflow header");
        return buff.readBytes((HEADER_SIZE-2));
    }
    
    // Netflow v5 flow records are 48 bytes each.
    private static ChannelBuffer getFlow(ChannelBuffer buff) throws CorruptDatagram {
        if (buff.readableBytes() < FLOW_SIZE)
            throw new CorruptDatagram();
        return buff.readBytes(FLOW_SIZE);
    }

}
