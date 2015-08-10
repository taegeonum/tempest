/*
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.tempest.example.storm.wordcount;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.snu.tempest.operator.window.timescale.TimescaleWindowOutput;
import edu.snu.tempest.operator.window.timescale.Timescale;

import java.util.Map;

/**
 * Serializer for MTSWindowOutput in WordCount.
 */
final class WordCountWindowOutputSerializer extends Serializer<TimescaleWindowOutput<Map<String, Long>>> {

  @Override
  public void write(final Kryo kryo,
                    final Output output,
                    final TimescaleWindowOutput<Map<String, Long>> mapMTSWindowOutput) {
    output.writeLong(mapMTSWindowOutput.timescale.windowSize);
    output.writeLong(mapMTSWindowOutput.timescale.intervalSize);
    kryo.writeObject(output, mapMTSWindowOutput.output);
    output.writeLong(mapMTSWindowOutput.startTime);
    output.writeLong(mapMTSWindowOutput.endTime);
    output.writeBoolean(mapMTSWindowOutput.fullyProcessed);
  }

  @Override
  public TimescaleWindowOutput<Map<String, Long>> read(final Kryo kryo,
                                                 final Input input,
                                                 final Class<TimescaleWindowOutput<Map<String, Long>>> aClass) {
    return new TimescaleWindowOutput<>(new Timescale(input.readLong(), input.readLong()),
        (Map<String, Long>)kryo.readObject(input, Map.class),
        input.readLong(),
        input.readLong(),
        input.readBoolean());
  }
}
