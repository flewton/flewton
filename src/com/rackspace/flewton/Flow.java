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

public class Flow {
    private static final String FLOW_TAG  = "flow";
    private static final String ATTR_TAG  = "attribute";
    private static final String NAME_TAG  = "name";
    private static final String VALUE_TAG = "value";

    public InetAddress sourceAddr;
    public InetAddress destAddr;
    public InetAddress nextHop;
    public short snmpIn;
    public short snmpOut;
    public int numPackets;
    public int numOctets;
    public int timeFirst;
    public int timeLast;
    public short sourcePort;
    public short destPort;
    public byte tcpFlags;
    public byte protocol;
    public byte tos;
    public short sourceAS;
    public short destAS;

    public long timestampCalculated;

    private static String wrapAttribute(String name, Object value) {
        return String.format(
                "<%s><%s>%s</%s><%s>%s</%s></%s>",
                ATTR_TAG,
                NAME_TAG,
                name,
                NAME_TAG,
                VALUE_TAG,
                value,
                VALUE_TAG,
                ATTR_TAG);
    }

    /**
     * Serialize flow to XML string.
     * 
     * @return XML representation of flow.
     */
    public String toXmlString() {
        StringBuilder out = new StringBuilder();

        out.append('<').append(FLOW_TAG).append('>');

        out.append(wrapAttribute("sourceAddr", sourceAddr.getHostAddress()));
        out.append(wrapAttribute("destAddr", destAddr.getHostAddress()));
        out.append(wrapAttribute("nextHop", nextHop.getHostAddress()));
        out.append(wrapAttribute("snmpIn", snmpIn));
        out.append(wrapAttribute("snmpOut", snmpOut));
        out.append(wrapAttribute("numPackets", numPackets));
        out.append(wrapAttribute("numOctets", numOctets));
        out.append(wrapAttribute("timeFirst", timeFirst));
        out.append(wrapAttribute("timeLast", timeLast));
        out.append(wrapAttribute("sourcePort", sourcePort));
        out.append(wrapAttribute("destPort", destPort));
        out.append(wrapAttribute("tcpFlags", tcpFlags));
        out.append(wrapAttribute("protocol", protocol));
        out.append(wrapAttribute("tos", tos));
        out.append(wrapAttribute("sourceAS", sourceAS));
        out.append(wrapAttribute("destAS", destAS));
        out.append(wrapAttribute("timestampCalculated", timestampCalculated));

        out.append("</").append(FLOW_TAG).append('>');

        return out.toString();
    }
}
