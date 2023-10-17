package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

//todo
// лучше избавится от asSlice
public class SSTableLoader {
    private static final MemorySegmentComparator comparator = MemorySegmentComparator.INSTANCE;
    private final Path ssTableFilePath;
    private final Path offsetFilePath;
    private long currentKeyOffset;

    public SSTableLoader(Path ssTableFilePath, Path offsetFilePath) {
        this.ssTableFilePath = ssTableFilePath;
        this.offsetFilePath = offsetFilePath;
    }

    public Entry<MemorySegment> findInSSTable(MemorySegment key) {
        if (ssTableFilePath == null || !Files.exists(ssTableFilePath)) return null;
        if (offsetFilePath == null || !Files.exists(offsetFilePath)) return null;

        long sstFileSize;
        long offsetFileSize;

        try {
            sstFileSize = Files.size(ssTableFilePath);
            offsetFileSize = Files.size(offsetFilePath);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        //todo
        // Эту арену нужно как-то закрыть
        Arena sstFileArena = Arena.ofConfined();

        try (
                FileChannel sstChannel = FileChannel.open(ssTableFilePath, StandardOpenOption.READ);
                FileChannel offsetChannel = FileChannel.open(offsetFilePath, StandardOpenOption.READ);
                Arena offsetFileArena = Arena.ofConfined();

        ) {
            MemorySegment sstFileMemorySegment = sstChannel.map(FileChannel.MapMode.READ_ONLY, 0, sstFileSize, sstFileArena);
            MemorySegment offsetFileMemorySegment = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetFileSize, offsetFileArena);

            var findValue = binarySearch(key,
                    offsetFileSize / (2L * Long.BYTES),
                    0,
                    sstFileMemorySegment,
                    offsetFileMemorySegment,
                    offsetFileSize);

            return findValue == null ? null : new BaseEntry<>(key, findValue);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MemorySegment binarySearch(MemorySegment key,
                                       long scopeSize,
                                       long startOffset,
                                       MemorySegment sstFileMemorySegment,
                                       MemorySegment offsetFileMemorySegment,
                                       long offsetFileSize
    ) throws IOException {
        if (scopeSize < 1) return null;
        MemorySegment keySegment;

        boolean isEven = scopeSize % 2 == 0;
        /// 1k 1v 2k 2v 3k 3v 4k 4v 5k 5v 6k 6v 7k 7v
        if (isEven) {
            currentKeyOffset = startOffset + Long.BYTES * scopeSize;
        } else {
            currentKeyOffset = startOffset + Long.BYTES * scopeSize - Long.BYTES;
        }
        keySegment = getKeyMemorySegment(sstFileMemorySegment, offsetFileMemorySegment);
        var compareResult = comparator.compare(key, keySegment);
        if (compareResult == 0) {
            return getValueMemorySegment(sstFileMemorySegment, offsetFileMemorySegment, offsetFileSize);
        } else if (compareResult < 0) {
            return binarySearch(key,
                    scopeSize / 2,
                    startOffset,
                    sstFileMemorySegment,
                    offsetFileMemorySegment,
                    offsetFileSize
            );
        } else {
            return binarySearch(key,
                    isEven ? scopeSize / 2 - 1 : scopeSize / 2,
                    currentKeyOffset + 2 * Long.BYTES,
                    sstFileMemorySegment,
                    offsetFileMemorySegment,
                    offsetFileSize
            );
        }
    }

    private MemorySegment getKeyMemorySegment(MemorySegment sstMemorySegment, MemorySegment offsetMemorySegment) {
        long sstOffset = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, currentKeyOffset);
        long sstOffset_to = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, currentKeyOffset + Long.BYTES);
        return sstMemorySegment.asSlice(sstOffset, sstOffset_to - sstOffset);
    }

    private MemorySegment getValueMemorySegment(MemorySegment sstMemorySegment, MemorySegment offsetMemorySegment, long offsetFileSize) {
        long sstOffset = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, currentKeyOffset + Long.BYTES);
        if (currentKeyOffset + 2 * Long.BYTES == offsetFileSize) {
            return sstMemorySegment.asSlice(sstOffset);
        } else {
            long sstOffset_to = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, currentKeyOffset + 2 * Long.BYTES);
            return sstMemorySegment.asSlice(sstOffset, sstOffset_to - sstOffset);
        }
    }
}
