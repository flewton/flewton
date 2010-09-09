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
package com.rackspace.flewton.test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;


public class SendFauxFlowsv5 {
    public static long bootedAt = System.currentTimeMillis();
    
	public static void main(String[] args) throws IOException, InterruptedException {
	    String hostName = "localhost";
	    int port = 9996;
	    
	    if (args.length > 0)
	        hostName = args[0];
	    if (args.length > 1)
	        port = Integer.valueOf(args[1]);
	    
		DatagramSocket clientSocket = new DatagramSocket();
		InetAddress addr = InetAddress.getByName(hostName);
		
		for (int i = 0; i < 1000; i++) {
		    byte[] record = makeRecord().array();
		    DatagramPacket packet = new DatagramPacket(record, record.length, addr, port);
		    clientSocket.send(packet);
		    if ((i % 10) == 0) {
		        System.out.println("Sent " + i + " records.");
		    }
		    Thread.sleep(500);
		}

		clientSocket.close();
	}
	
	public static ByteBuffer makeRecord() {
		ByteBuffer record = ByteBuffer.allocate(120);
		short version = 5, count = 2;
		record.putShort(version);
		record.putShort(count);
		record.putInt((int)(System.currentTimeMillis()-bootedAt));    // sys_uptime
		record.putInt((int)(System.currentTimeMillis()/1000));    // unix_secs
		record.putInt(0);    // No residual nanoseconds
		
		record.position(24);
		record.put(makeFlow());
		record.put(makeFlow());
		
		record.position(0);
		return record;
	}
	
	public static ByteBuffer makeFlow() {
		ByteBuffer flow = ByteBuffer.allocate(48);
		
		try {
			// src, dst, and nexthop
			flow.put(InetAddress.getByName("10.1.0.1").getAddress());
			flow.put(InetAddress.getByName("10.1.0.2").getAddress());
			flow.put(InetAddress.getByName("10.1.0.3").getAddress());
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
			
		// snmp indexes
		flow.putShort((short)5);
		flow.putShort((short)10);
		
		// packets and bytes
		flow.putInt(10);
		flow.putInt(100);
		
		// sysUptime start and end
		flow.putInt((int)(System.currentTimeMillis()-bootedAt));
		flow.putInt((int)(System.currentTimeMillis()-bootedAt)+1);
		
		// src and dst port
		flow.putShort((short)8888);
		flow.putShort((short)80);
		
		// padding
		flow.put((byte)0);
		
		// tcp flags, protocol, tos
		flow.put((byte)1);
		flow.put((byte)1);
		flow.put((byte)1);
		
		// src, dst AS number
		flow.putShort((short)500);
		flow.putShort((short)505);
			
		flow.position(0);
		return flow;
	}
}
