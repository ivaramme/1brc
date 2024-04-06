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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CalculateAverage_ivaramme {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {

        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }

        public String toString() {
            return "Station: " + station + ", value: " + value;
        }

    }

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        public String toString() {
            return min + " / " + sum + " / " + count + " / " + max;
        }

    }

    public static void main(String[] args) throws IOException {
        long start = System.currentTimeMillis();
        int workers = Runtime.getRuntime().availableProcessors();

        Map<String, MeasurementAggregator> resultMap = findSegments(workers).stream()
                .map(segment -> prepareSegmentForProcessing(segment))
                .parallel()
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey,
                        Map.Entry::getValue, (value1, value2) -> {
                            MeasurementAggregator agg = new MeasurementAggregator();
                            agg.count = value1.count + value2.count;
                            agg.max = Math.max(value1.max, value2.max);
                            agg.min = Math.min(value1.min, value2.min);
                            agg.sum = value1.sum + value2.sum;
                            return agg;
                        }));
        // System.out.println("Results: " + resultMap);
        System.out.println("File exploration took: " + (System.currentTimeMillis() - start));

        resultMap.forEach((k, v) -> System.out.println("Key: " + k + ", Value: " + v.count));
        System.out.println("Total entries in aggregate: " + resultMap.values().stream().mapToLong(k -> k.count).sum());
    }

    private static record FileSegment(
            long start,
            long end, int segmentNumber) {

        public String toString() {
            return "Segment # " + segmentNumber + " - start: " + start + ", end: " + end + ", lenght: " + length();
        }

        public long length() {
            return end - start;
        }
    }

    /**
     * Generates a list of file segments for later processing based on the number of
     * workers available to execute them. These segments are not equally
     * distributed, some are going to be larger than others given that the aim is to
     * contain complete entries instead of just partial entries.
     * 
     * <p>
     * Given that the data entry is small (data in file is just a few bytes per
     * entry) the difference in size of segments is not going to be that big.
     * </p>
     * 
     * @param workers
     * @return List of file segments with the start of the segment and the end.
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static List<FileSegment> findSegments(int workers) throws FileNotFoundException, IOException {
        try (RandomAccessFile raf = new RandomAccessFile(FILE, "r")) {
            final long size = raf.length();
            final long initialChunkSize = size / workers;

            long start = 0;
            long end = initialChunkSize;
            List<FileSegment> segments = new ArrayList<>();

            int segmentNumber = 0;
            while (start < size) {
                raf.seek(end);
                while (end < size && raf.read() != '\n') {
                    end++;
                }

                System.out.println(
                        "Creating segment: " + segmentNumber + " with start: " + start + " and end: " + end
                                + " Initial chunk size: " + initialChunkSize + " out of a file size of " + size);

                segments.add(new FileSegment(start, end, segmentNumber));

                start = end;
                end = Math.min(size, start + initialChunkSize);
                segmentNumber++;
            }

            System.out.println(segments);
            return segments;
        }
    }

    /**
     * Creates a MappedByteBuffer in disk based on the segment size and passed for
     * further processing.
     * 
     * @param segment metadata of the file segment to process
     * @return
     */
    private static Map<String, MeasurementAggregator> prepareSegmentForProcessing(FileSegment segment) {
        try (
                FileChannel channel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
            System.out.println("Preparing segment: " + segment);
            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, segment.start,
                    segment.length());
            return processEntriesInFileSegment(buffer, segment);
        } catch (IOException ioe) {
            System.out.println("Error opening file " + FILE);
        }

        return null;
    }

    /**
     * Performs the processing of entries, including the reading from the file and
     * the aggregation.
     * 
     * @param fileSegment
     * @param segmentDefinition
     * @return
     */
    private static Map<String, MeasurementAggregator> processEntriesInFileSegment(MappedByteBuffer fileSegment,
            FileSegment segmentDefinition) {
        System.out.println("Segment size: " + fileSegment.capacity() + ", number: " + segmentDefinition.segmentNumber
                + ". Thread name: " + Thread.currentThread().getName());

        Map<String, MeasurementAggregator> measurements = new HashMap<>();

        long offset = segmentDefinition.start;
        long end = segmentDefinition.end;
        final ByteBuffer bb = ByteBuffer.allocate(1024);
        int processed = 0;
        while (offset < end) {
            byte b = fileSegment.get();
            if (b == '\n' || offset + 1 == end) {
                bb.flip();

                String line = StandardCharsets.UTF_8.decode(bb).toString();
                if (!line.isEmpty()) {
                    Measurement m = new Measurement(line.split(";"));

                    MeasurementAggregator m1 = measurements.getOrDefault(m.station,
                            new MeasurementAggregator());
                    MeasurementAggregator m2 = new MeasurementAggregator();
                    m2.count = m1.count + 1;
                    m2.min = Math.min(m.value(), m1.min);
                    m2.max = Math.max(m.value(), m1.max);
                    m2.sum = m.value() + m1.sum;

                    measurements.put(m.station, m2);
                }
                bb.clear();
                processed++;
            } else {
                bb.put(b);
            }
            offset++;
        }

        System.out.println("Processed measurements for segment " + segmentDefinition.segmentNumber + ": " + processed);

        return measurements;
    }
}
