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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rackspace.flewton.backend.AbstractBackend;
import com.rackspace.flewton.backend.NullBackend;

public class CollectorHandler extends SimpleChannelHandler {
    private static final Logger logger = LoggerFactory.getLogger(CollectorHandler.class);
    private static final boolean logUnhandledVersions = Boolean.parseBoolean(System.getProperty("flewton.log_unhandled_versions", "false"));
    private static List<AbstractBackend> backEnds = new ArrayList<AbstractBackend>();
    private static final int HEX_LENGTH = 16;
    
    static {
        backEnds.add(new NullBackend(new HierarchicalConfiguration()));
    }
    
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
        ChannelBuffer buff = (ChannelBuffer)e.getMessage();
        int version = buff.readShort();
        
        logger.trace("received message of format v{}", version);
        
        AbstractRecord record =  null;
        try {
            switch (version) {
                case 5:
                    record = new Recordv5(buff);
                    break;
                default:
                    if (logUnhandledVersions) {
                        // this means the record stays null. we need to check for that when handling.
                        logger.warn(String.format("Netflow v%d is not supported", version));
                        if (logger.isDebugEnabled())
                            logPacket(e.getRemoteAddress(), buff.duplicate());
                    } else
                        throw new UnsupportedOperationException(
                                String.format("Netflow v%d is not supported", version));
            }
        } catch (CorruptDatagram err) {
            logger.error("Encountered a corrupt frame, skipping.");
            return;
        }
        
        // Send record to backends
        if (record != null)
            for (AbstractBackend backend : backEnds)
                backend.write(record);
    }
    
    // dumps the contents of a buffer
    private void logPacket(SocketAddress addr, ChannelBuffer buf) {
        // here's to the most useless abstract class in the JDK that I've come across so far...
        try {
            InetSocketAddress readAddr = (InetSocketAddress)addr;
            logger.debug("Logging packet from "+ readAddr.getAddress().getHostAddress() + ":" + readAddr.getPort());
        } catch (ClassCastException ex) {
            logger.warn("Couldn't cast " + addr.getClass().getName() + " to InetSocketAddress");
        }
        
        buf.resetReaderIndex();
        StringBuilder bytes = new StringBuilder();
        StringBuilder ascii = new StringBuilder(); // 32-126
        int pos = 0;
        String hex = null;
        while (buf.readable()) {
            byte b = buf.readByte();
            hex = Integer.toHexString(0x000000ff & b);
            if (hex.length() == 1)
                hex = "0" + hex;
            bytes.append(hex).append(" ");
            ascii.append(b >= 32 && b <= 126 ? (char)b : ".");
            pos++;
            if (pos == HEX_LENGTH) {
                pos = 0;
                logger.debug(bytes.toString() + " |" + ascii.toString() + "|");
                bytes = new StringBuilder();
                ascii = new StringBuilder();
            }
        }
        if (bytes.length() > 0) {
            while (pos++ < HEX_LENGTH) {
                bytes.append("   ");
                ascii.append(" ");
            }
            logger.debug(bytes.toString() + " |" + ascii.toString() + "|");
        }
    }
    
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        e.getCause().printStackTrace();
        //e.getChannel().close();
    }
    
    public static void setBackends(List<AbstractBackend> backends) {
        backEnds = backends;
    }
}
