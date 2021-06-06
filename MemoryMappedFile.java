import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;


public class MemoryMappedFile {

  
    private File file;

    private MappedByteBuffer mappedByteBuffer;
    
    private long fileExtendSize;

    private long fileLength;

    private long filePosition = 0L;

    private long mappedSize = 0L;

    private long offset = 0L;

    private long mappedPosition = 0L;

    public MemoryMappedFile(File file, long fileExtendSize) throws IOException {
        this.fileExtendSize = fileExtendSize;
        this.file = file;
        init();
    }

    public synchronized void init() throws IOException {
        this.mappedByteBuffer = mmap(this.file, FileChannel.MapMode.READ_WRITE, 0, this.fileExtendSize);
        this.fileLength = this.fileExtendSize;
    }

    private MappedByteBuffer mmap(File file, FileChannel.MapMode mode, long position, long size) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
             FileChannel fileChannel = randomAccessFile.getChannel()) {
            this.mappedSize = size;
            this.offset = 0L;
            this.mappedPosition = position;
            return fileChannel.map(mode, position, size);
        }
    }

    private void unmap(ByteBuffer cb) {
        final ByteBuffer byteBuffer = cb;
        CompletableFuture.runAsync(() -> {
            try {
                Method cleaner = byteBuffer.getClass().getMethod("cleaner");
                cleaner.setAccessible(true);
                Method clean = Class.forName("sun.misc.Cleaner").getMethod("clean");
                clean.setAccessible(true);
                clean.invoke(cleaner.invoke(byteBuffer));
            } catch (NoSuchMethodException | ClassNotFoundException | IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException("get buffuer cleaner failed", e);
            }
        });
    }


    public synchronized void write(byte[] data) throws IOException {
        try {
            if (data.length > mappedByteBuffer.remaining()) {
                unmap(mappedByteBuffer);
                this.fileLength += this.fileExtendSize;
                mappedByteBuffer = mmap(file,
                        FileChannel.MapMode.READ_WRITE,
                        filePosition,
                        this.fileLength - filePosition);
            }
            mappedByteBuffer.put(data);
            this.filePosition += data.length;
        } catch (BufferOverflowException e) {
            throw new IOException(e);
        }
    }

    public synchronized void seek(long pos) throws IOException {
        if(pos < this.mappedPosition || pos >= this.mappedPosition + this.mappedSize) {
            long fileLen = file.length();
            if (pos > fileLen) {
                throw new IOException("position (" + pos + ") exceed file length (" + file.length() + ")");
            }
            unmap(mappedByteBuffer);
            if (filePosition + fileExtendSize <= fileLen) {
                mappedByteBuffer = mmap(file, FileChannel.MapMode.READ_WRITE, filePosition, fileExtendSize);
            } else {
                mappedByteBuffer = mmap(file,
                        FileChannel.MapMode.READ_WRITE,
                        filePosition,
                        fileLen - filePosition);
            }
        }
        this.filePosition = pos;
    }

    public synchronized void read(byte[] buffer) throws IOException {
        int len = buffer.length;
        if (this.offset + len > this.mappedSize) {
            long fileLen = file.length();
            if (filePosition + len > fileLen) {
                throw new EOFException("read data at " + filePosition + " size " + len + " exceed file length (" + file.length() + ")");
            }
            if (len > this.fileExtendSize) {
                throw new IOException("read data len(" + len + ") exceed max map size (" + this.fileExtendSize + ")");
            }
            if (filePosition + fileExtendSize <= fileLen) {
                unmap(mappedByteBuffer);
                mappedByteBuffer = mmap(file, FileChannel.MapMode.READ_WRITE, filePosition, fileExtendSize);
            } else {
                if (len > this.fileLength - this.filePosition) {
                    throw new EOFException("read data len(" + len + ") exceed file");
                }
                mappedByteBuffer = mmap(file,
                        FileChannel.MapMode.READ_WRITE,
                        filePosition,
                        fileLen - filePosition);
            }
        }
        loadMessage(len);
        mappedByteBuffer.get(buffer);
        filePosition += len;
        offset += len;
    }

    private void loadMessage(int needBytes) {
        while (mappedByteBuffer.remaining() < needBytes) {
            mappedByteBuffer.load();
        }
    }


    public void flush() {
        if (mappedByteBuffer != null) {
            mappedByteBuffer.force();
        }
    }


}
