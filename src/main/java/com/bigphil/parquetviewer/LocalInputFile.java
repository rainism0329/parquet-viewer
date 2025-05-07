package com.bigphil.parquetviewer;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;

public class LocalInputFile implements InputFile {
    private final File file;

    public LocalInputFile(File file) {
        this.file = file;
    }

    @Override
    public long getLength() {
        return file.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        RandomAccessFile raf = new RandomAccessFile(file, "r");
        FileChannel channel = raf.getChannel();

        return new DelegatingSeekableInputStream(Channels.newInputStream(channel)) {
            @Override
            public long getPos() throws IOException {
                return channel.position();
            }

            @Override
            public void seek(long newPos) throws IOException {
                channel.position(newPos);
            }

            @Override
            public void close() throws IOException {
                super.close();
                channel.close();
                raf.close();
            }
        };
    }
}
