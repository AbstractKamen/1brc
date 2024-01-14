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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Initial submission 38s - file segmentation, parallel processing
 *
 * Improvements:
 *      - smarter value parsing (either #.# or ##.#)                        32s
 *
 *      - MemorySegment preview feature                                     27s
 *
 *      - simple hash table with linear probing and simple hash function    24s
 *
 *      - aggregate simple hash tables instead of making HashMaps, make the
 *      station String after aggregating and then sort with Array.sort(),
 *      make final String with a single StringBuilder                       18s
 *
 *      - copy pasta station parsing for case where simple hash has a hit in
 *      order to avoid System.arraycopy() - just comparing with current values
 *      is ok                                                               15s
 */

public class CalculateAverage_AbstractKamen {

    private static final String FILE = "./measurements.txt";
    public static final int TABLE_LENGTH = 1_048_576;
    public static final int TABLE_LENGTH_MASK = 1_048_575;

    private static class Measurement {
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;
        private int sum;
        private long count = 1;
        private byte[] station;
        private String stationString;
        private int hash;

        public void setStationString() {
            stationString = new String(station, StandardCharsets.UTF_8);
        }

    }

    public static void main(String[] args) throws IOException {
        try (final FileChannel fc = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ);
                final RandomAccessFile raf = new RandomAccessFile(new File(FILE), "r")) {
            getParallelMemorySegmentStream(raf, fc)
                    .map(CalculateAverage_AbstractKamen::getMeasurements)
                    .reduce(CalculateAverage_AbstractKamen::aggregateMeasurementHashTables)
                    .map(measurements -> {
                        final int count = getMeasurementsCount_andSetStationString(measurements);
                        final Measurement[] sortedMeasurements = getSortedMeasurements(measurements, count);
                        return getResultString(count, sortedMeasurements);
                    })
                    .ifPresent(System.out::println);
        }
    }

    private static Stream<MemorySegment> getParallelMemorySegmentStream(RandomAccessFile raf, FileChannel fc) throws IOException {
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                new MemorySegmentIterator(raf, fc, availableProcessors),
                Spliterator.IMMUTABLE),
                true);
    }

    private static Measurement[] getMeasurements(MemorySegment memorySegment) {
        final Measurement[] table = new Measurement[TABLE_LENGTH];
        final byte[] buffer = new byte[256];
        final long limit = memorySegment.byteSize();
        long i = 0;
        while (i < limit) {
            byte b;
            int nameLen = 0;
            int hash = 0;
            while ((b = getByte(memorySegment, i++)) != ';') {
                buffer[nameLen++] = b;
                hash = simpleHash(hash, b, nameLen);
            }
            i = addOrUpdate(memorySegment, i, buffer, table, hash, nameLen);
        }
        return table;
    }

    private static long addOrUpdate(MemorySegment segment, long i, byte[] buffer, Measurement[] table, int hash, int nameLen) {
        int index = hash & TABLE_LENGTH_MASK;
        Measurement measurement;
        // linear probing stuff
        while ((measurement = table[index]) != null) {
            if (arrayEquals(measurement.station, buffer, nameLen)) {
                break;
            }
            index = (index + 1) & TABLE_LENGTH_MASK;
        }

        if (measurement != null) {
            // copy pasta deluxe
            byte b;
            int valueLen = 0;
            int neg = 1;
            while (((b = getByte(segment, i++)) != '\n')) {
                // skip the dot and retart char
                if (b == '.' || b == '\r')
                    continue;
                if (b == '-') {
                    neg = -1;
                }
                else {
                    buffer[valueLen++] = b;
                }
            }
            final int val = parsers[valueLen].applyAsInt(buffer) * neg;
            measurement.min = Math.min(measurement.min, val);
            measurement.max = Math.max(measurement.max, val);
            measurement.sum += val;
            measurement.count++;
        }
        else {
            final byte[] station = new byte[nameLen];
            System.arraycopy(buffer, 0, station, 0, nameLen);
            byte b;
            int valueLen = 0;
            int neg = 1;
            while (((b = getByte(segment, i++)) != '\n')) {
                // skip the dot and retart char
                if (b == '.' || b == '\r')
                    continue;
                if (b == '-') {
                    neg = -1;
                }
                else {
                    buffer[valueLen++] = b;
                }
            }
            final int val = parsers[valueLen].applyAsInt(buffer) * neg;
            measurement = new Measurement();
            measurement.min = val;
            measurement.max = val;
            measurement.sum = val;
            measurement.station = station;
            measurement.hash = hash;
            table[index] = measurement;
        }
        return i;
    }

    private static Measurement[] aggregateMeasurementHashTables(Measurement[] src, Measurement[] target) {
        for (int i = 0; i < TABLE_LENGTH; i++) {
            final Measurement s = src[i];
            if (s != null) {
                int index = s.hash & TABLE_LENGTH_MASK;
                Measurement t;
                // linear probing stuff
                while ((t = target[index]) != null) {
                    if (t.station.length == s.station.length
                            && arrayEquals(t.station, s.station, t.station.length)) {
                        break;
                    }
                    index = (index + 1) & TABLE_LENGTH_MASK;
                }
                if (t != null) {
                    t.min = Math.min(s.min, t.min);
                    t.max = Math.max(s.max, t.max);
                    t.sum += s.sum;
                    t.count += s.count;
                }
                else {
                    target[index] = s;
                }
            }
        }
        return target;
    }

    private static String getResultString(int count, Measurement[] sortedMeasurements) {
        final StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < count - 1; i++) {
            final Measurement m = sortedMeasurements[i];
            appendMeasurement(sb, m).append(", ");
        }

        final Measurement m = sortedMeasurements[count - 1];
        appendMeasurement(sb, m).append("}");
        return sb.toString();
    }

    private static Measurement[] getSortedMeasurements(Measurement[] measurements, int count) {
        final Measurement[] presentMeasurements = new Measurement[count];
        for (int i = 0, j = 0; i < measurements.length; i++) {
            final Measurement m = measurements[i];
            if (m != null) {
                presentMeasurements[j++] = m;
            }
        }

        Arrays.sort(presentMeasurements, Comparator.comparing(m -> m.stationString));
        return presentMeasurements;
    }

    private static int getMeasurementsCount_andSetStationString(Measurement[] measurements) {
        int count = 0;
        for (int i = 0; i < measurements.length; i++) {
            final Measurement m = measurements[i];
            if (m != null) {
                count++;
                m.setStationString();
            }
        }
        return count;
    }

    private static StringBuilder appendMeasurement(StringBuilder sb, Measurement m) {
        return sb.append(m.stationString)
                .append("=")
                .append(round(m.min / 10.0))
                .append("/")
                .append(round(m.sum / 10.0 / m.count))
                .append("/")
                .append(round(m.max / 10.0));
    }

    private static double round(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static int simpleHash(int hash, byte b, int nameLen) {
        return hash + (17 * b) + nameLen;
    }

    private static byte getByte(MemorySegment memorySegment, long i) {
        return memorySegment.get(ValueLayout.JAVA_BYTE, i);
    }

    private static final IntParser[] parsers = new IntParser[]{ CalculateAverage_AbstractKamen::getVal_0,
            CalculateAverage_AbstractKamen::getVal_0,
            CalculateAverage_AbstractKamen::getVal_2, CalculateAverage_AbstractKamen::getVal_3 };

    private static int getVal_3(byte[] bytes) {
        return (bytes[0] - 48) * 100 + (bytes[1] - 48) * 10 + (bytes[2] - 48);
    }

    private static int getVal_2(byte[] bytes) {
        return (bytes[0] - 48) * 10 + (bytes[1] - 48);
    }

    private static int getVal_0(byte[] bytes) {
        return 0;
    }

    private static boolean arrayEquals(byte[] station, byte[] bytes, int nameLen) {
        for (int i = 0; i < nameLen; i++) {
            if (station[i] != bytes[i])
                return false;
        }
        return true;
    }

}

interface IntParser extends ToIntFunction<byte[]> {
}

class MemorySegmentIterator implements Iterator<MemorySegment> {
    private long start;
    private final RandomAccessFile raf;
    private final FileChannel fc;
    private final long fileLength;
    private final long chunkSize;

    public MemorySegmentIterator(RandomAccessFile raf, FileChannel fc, int numberOfParts) throws IOException {
        this.raf = raf;
        this.fc = fc;
        this.fileLength = fc.size();
        this.chunkSize = fileLength / numberOfParts;
    }

    @Override
    public boolean hasNext() {
        return start < fileLength;
    }

    @Override
    public MemorySegment next() {
        try {
            if (hasNext()) {
                final long end = getEnd();
                long position = start;
                this.start = end;
                return fc.map(MapMode.READ_ONLY, position, end - position, Arena.ofShared());
            }
            else {
                throw new NoSuchElementException();
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long getEnd() throws IOException {
        long end = Math.min(start + chunkSize, fileLength);
        while (end < fileLength) {
            raf.seek(end++);
            if (raf.read() == '\n')
                break;
        }
        return end;
    }

}