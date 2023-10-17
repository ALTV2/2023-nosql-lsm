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
public class SSTableLoader {
    private static final MemorySegmentComparator comparator = MemorySegmentComparator.INSTANCE;
    private final Path ssTableFilePath;
    private final Path offsetFilePath;
    private long SSTFileOffset;
    private long offsetFileOffset;

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

        SSTFileOffset = 0;
        offsetFileOffset = 0;

        MemorySegment lastMemorySegment = null;
        MemorySegment keySegment = null;

        Arena sstFileArena = Arena.ofConfined();
        Arena offsetFileArena = Arena.ofConfined();

        try (
             FileChannel sstChannel = FileChannel.open(ssTableFilePath, StandardOpenOption.READ);
             FileChannel offsetChannel = FileChannel.open(offsetFilePath, StandardOpenOption.READ)
        ) {
            MemorySegment sstFileMemorySegment = sstChannel.map(FileChannel.MapMode.READ_ONLY, 0, sstFileSize, sstFileArena);
            MemorySegment offsetFileMemorySegment = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetFileSize, offsetFileArena);

            while (SSTFileOffset < sstFileSize) {
                keySegment = getKeyMemorySegment(sstFileMemorySegment, offsetFileMemorySegment);
                if (comparator.compare(key, keySegment) == 0) {
                    lastMemorySegment = getValueMemorySegment(sstFileMemorySegment, offsetFileMemorySegment);
                    break;
                } else skipValue(offsetFileMemorySegment);
            }

            return lastMemorySegment == null ? null : new BaseEntry<>(keySegment, lastMemorySegment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MemorySegment getKeyMemorySegment(MemorySegment sstMemorySegment, MemorySegment offsetMemorySegment) throws IOException {
        long size = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, offsetFileOffset);
        var mm = sstMemorySegment.asSlice(SSTFileOffset, size);
        SSTFileOffset += size;
        offsetFileOffset += Long.BYTES;
        return  mm;
    }

    private MemorySegment getValueMemorySegment(MemorySegment sstMemorySegment, MemorySegment offsetMemorySegment) throws IOException {
        long size = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, offsetFileOffset);
        return sstMemorySegment.asSlice(SSTFileOffset, size);
    }

    private void skipValue(MemorySegment offsetMemorySegment) throws IOException {
        long size = offsetMemorySegment.get(ValueLayout.OfLong.JAVA_LONG, offsetFileOffset);
        SSTFileOffset += size;
        offsetFileOffset += Long.BYTES;
    }
}
