/*
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flinkspector.core.runtime;

import com.lmax.disruptor.EventTranslatorOneArg;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteEventTranslator implements EventTranslatorOneArg<ByteEvent, ByteBuffer> {

    public void translateTo(ByteEvent event, long sequence, ByteBuffer bb) {
        int address = bb.getInt(0);
        bb.position(4);
        byte[] msg = new byte[bb.remaining()];
        bb.get(msg);
        event.set(address, msg);
    }

    public static ByteBuffer translateToBuffer(int address, byte[] msg) {
        ByteBuffer bb = ByteBuffer.allocate(msg.length + 4);
        bb.putInt(address);
        bb.put(msg);
        return bb;
    }
}

