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

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UUIDTests {
    
    @Test
    public void testUUIDs() {
        List<UUID> ids = new ArrayList<UUID>();
        
        long now = 1292510156570L;
        
        // make sure that each UUID is unique and greater than the last one generated.
        UUID last = null;
        for (int i = 0; i < 9999; i++) {
            ByteBuffer bb = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(now));
            UUID uuid = new UUID(bb.getLong(), bb.getLong());
            assert uuid.version() == 1;
            assert !ids.contains(uuid);
            ids.add(uuid);
            if (last != null)
                assert uuid.compareTo(last) > 0;
            last = uuid;
        }
        
        // even when times goes forward.
        now++;
        for (int i = 0; i < 9999; i++) {
            ByteBuffer bb = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(now));
            UUID uuid = new UUID(bb.getLong(), bb.getLong());
            assert uuid.version() == 1;
            assert !ids.contains(uuid);
            ids.add(uuid);
            assert uuid.compareTo(last) > 0;
            last = uuid;
        }
        
        // even when time goes back on itself.
        now--;
        for (int i = 0; i < 9999; i++) {
            ByteBuffer bb = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(now));
            UUID uuid = new UUID(bb.getLong(), bb.getLong());
            assert uuid.version() == 1;
            assert !ids.contains(uuid);
            ids.add(uuid);
            assert uuid.compareTo(last) > 0;
            last = uuid;
        }
        
        // even when time goes really backwards.
        now--;
        for (int i = 0; i < 9999; i++) {
            ByteBuffer bb = ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(now));
            UUID uuid = new UUID(bb.getLong(), bb.getLong());
            assert uuid.version() == 1;
            assert !ids.contains(uuid);
            ids.add(uuid);
            assert uuid.compareTo(last) > 0;
            last = uuid;
        }
    }
}