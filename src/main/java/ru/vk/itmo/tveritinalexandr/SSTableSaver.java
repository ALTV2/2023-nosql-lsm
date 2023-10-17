package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SortedMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public class SSTableSaver {
    private final SortedMap<MemorySegment, Entry<MemorySegment>> memTable;
    private final Path ssTableFilePath;
    private final Path offsetPath;
    private long SSTFileOffset;
    private long offsetFileOffset;

    public SSTableSaver(Path ssTableFilePath, Path offsetPath, SortedMap<MemorySegment, Entry<MemorySegment>> memTable) {
        this.ssTableFilePath = ssTableFilePath;
        this.offsetPath = offsetPath;
        this.memTable = memTable;
    }

    public void save() throws IOException {
        try (FileChannel SSTChannel = FileChannel.open(ssTableFilePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);

             FileChannel OffsetChannel = FileChannel.open(offsetPath,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING,
                     StandardOpenOption.CREATE);

             Arena SSTArena = Arena.ofConfined();
             Arena OffsetArena = Arena.ofConfined()
        ) {
            SSTFileOffset = 0;
            offsetFileOffset = 0;

            MemorySegment offsetMemorySegment = OffsetChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcOffsetFileSize(memTable), OffsetArena);
            MemorySegment SSTMemorySegment = SSTChannel.map(FileChannel.MapMode.READ_WRITE, 0, calcFileSize(memTable), SSTArena);

            for (var entry : memTable.values()) {
                fillSSTMemorySegment(entry.key(), SSTMemorySegment, offsetMemorySegment);
                fillSSTMemorySegment(entry.value(), SSTMemorySegment, offsetMemorySegment);
            }

            SSTMemorySegment.load();
            offsetMemorySegment.load();
        }
    }

    private long calcOffsetFileSize(SortedMap<MemorySegment, Entry<MemorySegment>> inMemoryDB) {
        return 2L * Long.BYTES * inMemoryDB.size();
    }

    private long calcFileSize(SortedMap<MemorySegment, Entry<MemorySegment>> inMemoryDB) {
        long size = 0;
        for (var entry : inMemoryDB.values()) {
            size += (entry.key().byteSize() + entry.value().byteSize());
        }
        return size;
    }

    private void fillSSTMemorySegment(MemorySegment memTablememorySegment, MemorySegment SSTMemorySegment, MemorySegment offsetMemorySegment) {
        long size = memTablememorySegment.byteSize();
        MemorySegment.copy(memTablememorySegment, 0, SSTMemorySegment, SSTFileOffset, size);
        saveOffsetInfo(offsetMemorySegment, SSTFileOffset);
        SSTFileOffset += size;
    }

    private void saveOffsetInfo(MemorySegment offsetMemorySegment, long offset) {
        offsetMemorySegment.set(JAVA_LONG_UNALIGNED, offsetFileOffset, offset);
        offsetFileOffset += Long.BYTES;
    }
}
