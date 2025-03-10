/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import sun.misc.Unsafe;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

public class CalculateAverage_zerninv {
    private static final String FILE = "./measurements.txt";
    private static final int CORES = Runtime.getRuntime().availableProcessors();
    private static final int CHUNK_SIZE = 1024 * 1024 * 32;

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            Field unsafe = Unsafe.class.getDeclaredField("theUnsafe");
            unsafe.setAccessible(true);
            return (Unsafe) unsafe.get(Unsafe.class);
        }
        catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        try (var channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            var fileSize = channel.size();
            var minChunkSize = Math.min(fileSize, CHUNK_SIZE);
            var segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());

            var tasks = new TaskThread[CORES];
            for (int i = 0; i < tasks.length; i++) {
                tasks[i] = new TaskThread(new MeasurementContainer(), (int) (fileSize / minChunkSize / CORES + 1));
            }

            var chunks = splitByChunks(segment.address(), segment.address() + fileSize, minChunkSize);
            for (int i = 0; i < chunks.size() - 1; i++) {
                var task = tasks[i % tasks.length];
                task.addChunk(chunks.get(i), chunks.get(i + 1));
            }

            for (var task : tasks) {
                task.start();
            }

            var results = new TreeMap<String, TemperatureAggregation>();
            for (var task : tasks) {
                task.join();
                task.measurements()
                        .forEach(measurement -> {
                            var aggr = results.get(measurement.station());
                            if (aggr == null) {
                                results.put(measurement.station(), measurement.aggregation());
                            }
                            else {
                                aggr.merge(measurement.aggregation());
                            }
                        });
            }

            var bos = new BufferedOutputStream(System.out);
            bos.write(new TreeMap<>(results).toString().getBytes(StandardCharsets.UTF_8));
            bos.write('\n');
            bos.flush();
        }
    }

    private static List<Long> splitByChunks(long address, long end, long minChunkSize) {
        List<Long> result = new ArrayList<>((int) ((end - address) / minChunkSize + 1));
        result.add(address);
        while (address < end) {
            address += Math.min(end - address, minChunkSize);
            while (address < end && UNSAFE.getByte(address++) != '\n') {
            }
            result.add(address);
        }
        return result;
    }

    private static final class TemperatureAggregation {
        private long sum;
        private int count;
        private short min;
        private short max;

        public TemperatureAggregation(long sum, int count, short min, short max) {
            this.sum = sum;
            this.count = count;
            this.min = min;
            this.max = max;
        }

        public void merge(TemperatureAggregation o) {
            if (o == null) {
                return;
            }
            sum += o.sum;
            count += o.count;
            min = min < o.min ? min : o.min;
            max = max > o.max ? max : o.max;
        }

        @Override
        public String toString() {
            return min / 10d + "/" + Math.round(sum / 1d / count) / 10d + "/" + max / 10d;
        }
    }

    private record Measurement(String station, TemperatureAggregation aggregation) {
    }

    private static final class MeasurementContainer {
        private static final int SIZE = 1 << 17;

        private static final int ENTRY_SIZE = 4 + 4 + 8 + 1 + 8 + 8 + 2 + 2;
        private static final int COUNT_OFFSET = 0;
        private static final int HASH_OFFSET = 4;
        private static final int LAST_BYTES_OFFSET = 8;
        private static final int SIZE_OFFSET = 16;
        private static final int ADDRESS_OFFSET = 17;
        private static final int SUM_OFFSET = 25;
        private static final int MIN_OFFSET = 33;
        private static final int MAX_OFFSET = 35;

        private final long address;

        private MeasurementContainer() {
            address = UNSAFE.allocateMemory(ENTRY_SIZE * SIZE);
            UNSAFE.setMemory(address, ENTRY_SIZE * SIZE, (byte) 0);
        }

        public void put(long address, byte size, int hash, long lastBytes, short value) {
            int idx = Math.abs(hash % SIZE);
            long ptr = this.address + idx * ENTRY_SIZE;
            int count;
            boolean fastEqual;

            while ((count = UNSAFE.getInt(ptr + COUNT_OFFSET)) != 0) {
                fastEqual = UNSAFE.getInt(ptr + HASH_OFFSET) == hash && UNSAFE.getLong(ptr + LAST_BYTES_OFFSET) == lastBytes;
                if (fastEqual && UNSAFE.getByte(ptr + SIZE_OFFSET) == size && isEqual(UNSAFE.getLong(ptr + ADDRESS_OFFSET), address, size - 8)) {

                    UNSAFE.putInt(ptr + COUNT_OFFSET, count + 1);
                    UNSAFE.putLong(ptr + ADDRESS_OFFSET, address);
                    UNSAFE.putLong(ptr + SUM_OFFSET, UNSAFE.getLong(ptr + SUM_OFFSET) + value);
                    if (value < UNSAFE.getShort(ptr + MIN_OFFSET)) {
                        UNSAFE.putShort(ptr + MIN_OFFSET, value);
                    }
                    if (value > UNSAFE.getShort(ptr + MAX_OFFSET)) {
                        UNSAFE.putShort(ptr + MAX_OFFSET, value);
                    }
                    return;
                }
                idx = (idx + 1) % SIZE;
                ptr = this.address + idx * ENTRY_SIZE;
            }

            UNSAFE.putInt(ptr + COUNT_OFFSET, 1);
            UNSAFE.putInt(ptr + HASH_OFFSET, hash);
            UNSAFE.putLong(ptr + LAST_BYTES_OFFSET, lastBytes);
            UNSAFE.putByte(ptr + SIZE_OFFSET, size);
            UNSAFE.putLong(ptr + ADDRESS_OFFSET, address);

            UNSAFE.putLong(ptr + SUM_OFFSET, value);
            UNSAFE.putShort(ptr + MIN_OFFSET, value);
            UNSAFE.putShort(ptr + MAX_OFFSET, value);
        }

        public List<Measurement> measurements() {
            var result = new ArrayList<Measurement>(1000);
            int count;
            for (int i = 0; i < SIZE; i++) {
                long ptr = this.address + i * ENTRY_SIZE;
                count = UNSAFE.getInt(ptr + COUNT_OFFSET);
                if (count != 0) {
                    var station = createString(UNSAFE.getLong(ptr + ADDRESS_OFFSET), UNSAFE.getByte(ptr + SIZE_OFFSET));
                    var measurements = new TemperatureAggregation(
                            UNSAFE.getLong(ptr + SUM_OFFSET),
                            count,
                            UNSAFE.getShort(ptr + MIN_OFFSET),
                            UNSAFE.getShort(ptr + MAX_OFFSET));
                    result.add(new Measurement(station, measurements));
                }
            }
            return result;
        }

        private boolean isEqual(long address, long address2, int size) {
            for (int i = 0; i < size; i += 8) {
                if (UNSAFE.getLong(address + i) != UNSAFE.getLong(address2 + i)) {
                    return false;
                }
            }
            return true;
        }

        private String createString(long address, byte size) {
            byte[] arr = new byte[size];
            for (int i = 0; i < size; i++) {
                arr[i] = UNSAFE.getByte(address + i);
            }
            return new String(arr);
        }
    }

    private static class TaskThread extends Thread {
        // #.##
        private static final int THREE_DIGITS_MASK = 0x2e0000;
        // #.#
        private static final int TWO_DIGITS_MASK = 0x2e00;
        // #.#-
        private static final int TWO_NEGATIVE_DIGITS_MASK = 0x2e002d;
        private static final int BYTE_MASK = 0xff;

        private static final int ZERO = '0';
        private static final byte DELIMITER = ';';

        private final MeasurementContainer container;
        private final List<Long> begins;
        private final List<Long> ends;

        private TaskThread(MeasurementContainer container, int chunks) {
            this.container = container;
            this.begins = new ArrayList<>(chunks);
            this.ends = new ArrayList<>(chunks);
        }

        public void addChunk(long begin, long end) {
            begins.add(begin);
            ends.add(end);
        }

        @Override
        public void run() {
            for (int i = 0; i < begins.size(); i++) {
                calcForChunk(begins.get(i), ends.get(i));
            }
        }

        public List<Measurement> measurements() {
            return container.measurements();
        }

        private void calcForChunk(long offset, long end) {
            long cityOffset, lastBytes;
            int hashCode, temperature, word;
            byte cityNameSize, b;

            while (offset < end) {
                cityOffset = offset;
                lastBytes = 0;
                hashCode = 0;
                while ((b = UNSAFE.getByte(offset++)) != DELIMITER) {
                    hashCode += hashCode * 31 + b;
                    lastBytes = (lastBytes << 8) | b;
                }
                cityNameSize = (byte) (offset - cityOffset - 1);

                word = UNSAFE.getInt(offset);
                offset += 4;

                if ((word & TWO_NEGATIVE_DIGITS_MASK) == TWO_NEGATIVE_DIGITS_MASK) {
                    word >>>= 8;
                    temperature = ZERO * 11 - ((word & BYTE_MASK) * 10 + ((word >>> 16) & BYTE_MASK));
                }
                else if ((word & THREE_DIGITS_MASK) == THREE_DIGITS_MASK) {
                    temperature = (word & BYTE_MASK) * 100 + ((word >>> 8) & BYTE_MASK) * 10 + ((word >>> 24) & BYTE_MASK) - ZERO * 111;
                }
                else if ((word & TWO_DIGITS_MASK) == TWO_DIGITS_MASK) {
                    temperature = (word & BYTE_MASK) * 10 + ((word >>> 16) & BYTE_MASK) - ZERO * 11;
                    offset--;
                }
                else {
                    // #.##-
                    word = (word >>> 8) | (UNSAFE.getByte(offset++) << 24);
                    temperature = ZERO * 111 - ((word & BYTE_MASK) * 100 + ((word >>> 8) & BYTE_MASK) * 10 + ((word >>> 24) & BYTE_MASK));
                }
                offset++;
                container.put(cityOffset, cityNameSize, hashCode, lastBytes, (short) temperature);
            }
        }
    }
}
