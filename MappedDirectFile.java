import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;

/**
 * Class for direct access to a memory mapped file.
 */
@SuppressWarnings("restriction")
public class MappedDirectFile {

    private static final Unsafe unsafe;
    private static final Method mmap;
    private static final Method unmmap;
    private static final int BYTE_ARRAY_OFFSET;
    private final File file;
    private long fileExtendSize;
    private long fileLen;
    private long filePosition;
    private long mappedSize;
    private long mappedAddr;
    private long mappedAddrOffset;
    private long mappedPosition;

    static {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            unsafe = (Unsafe) theUnsafe.get(null);
            mmap = getMethod(FileChannelImpl.class, "map0", int.class, long.class, long.class);
            unmmap = getMethod(FileChannelImpl.class, "unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = unsafe.arrayBaseOffset(byte[].class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs a new memory mapped file.
     *
     * @param file           the file object
     * @param fileExtendSize the  fileExtendSize
     * @throws IOException in case there was an error creating the memory mapped file
     */
    public MappedDirectFile(File file, long fileExtendSize) throws IOException {
        this.file = file;
        this.fileExtendSize = roundTo4096(fileExtendSize);
        this.fileLen = this.fileExtendSize;
        this.filePosition = 0L;
        mmap(0, fileExtendSize);
    }


    /**
     * Reads a buffer of data.
     *
     * @param data the input buffer
     */
    public synchronized void read(byte[] data) throws IOException {
        int len = data.length;
        if (this.mappedAddrOffset + len > this.mappedSize) {
            long fileLength = file.length();
            if (this.mappedAddrOffset + len > fileLength) {
                throw new EOFException("read data at (" + this.mappedAddrOffset + ") size (" + len + ") exceed file length (" + fileLength + ")");
            }
            if (len > this.fileExtendSize) {
                throw new IOException("read data len(" + len + ") exceed max map size (" + this.fileExtendSize + ")");
            }
            if (this.filePosition + this.fileExtendSize <= this.fileLen) {
                unmap(this.mappedAddr, mappedSize);
                mmap(this.filePosition, this.fileExtendSize);
            } else {
                if (len > this.fileLen - this.filePosition) {
                    throw new EOFException("read data len(" + len + ") exceed file");
                }
                unmap(this.mappedAddr, mappedSize);
                mmap(this.filePosition, this.fileLen - this.filePosition);
            }
        }
        unsafe.copyMemory(null, this.mappedAddr + this.filePosition, data, BYTE_ARRAY_OFFSET, len);
        this.filePosition += len;
        this.mappedAddrOffset = len;
    }

    /**
     * Writes a buffer of data.
     *
     * @param data the output buffer
     */
    public synchronized void write(byte[] data) throws IOException {
        int len = data.length;
        if (this.filePosition + len > this.mappedSize) {
            unmap(this.mappedAddr, this.mappedSize);
            this.fileLen += this.fileExtendSize;
            mmap(this.filePosition, this.fileLen - this.filePosition);
        }
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET, null, filePosition + mappedAddr, len);
        this.filePosition += len;
        this.mappedAddrOffset += len;
    }

    public synchronized void seek(long pos) {
        try {
            if (pos < this.mappedPosition || pos >= this.mappedPosition + this.mappedSize) {
                long fileLength = this.file.length();
                if (pos > fileLength) {
                    throw new IOException("seek (" + pos + ") exceed file length (" + fileLength + ")");
                }
                unmap(this.mappedAddr, this.mappedSize);
                if (this.filePosition + this.fileExtendSize <= this.fileLen) {
                    mmap(this.filePosition, this.fileExtendSize);
                } else {
                    mmap(this.filePosition, this.fileLen - this.filePosition);
                }
            }
            this.filePosition = pos;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void mmap(long position, long size) throws IOException {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "rw");
             FileChannel fileChannel = randomAccessFile.getChannel()) {
            if (randomAccessFile.length() < this.fileLen) {
                randomAccessFile.setLength(this.fileLen);
            }
            this.mappedSize = roundTo4096(size);
            this.mappedAddr = (long) mmap.invoke(fileChannel, 1, position, mappedSize);
            this.mappedPosition = position;
            this.mappedAddrOffset = 0;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private synchronized void unmap(final long addr, final long mappedSize) {
        CompletableFuture.runAsync(() -> {
            try {
                unmmap.invoke(null, addr, mappedSize);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static Method getMethod(Class<?> cls, String name, Class<?>... params) throws Exception {
        Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    private static long roundTo4096(long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    /**
     * Reads a byte from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public byte getByte(long pos) {
        seek(pos);
        return unsafe.getByte(this.mappedAddr + this.filePosition);
    }

    /**
     * Reads a byte (volatile) from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public byte getByteVolatile(long pos) {
        seek(pos);
        return unsafe.getByteVolatile(null, this.mappedAddr + this.filePosition);
    }

    /**
     * Reads an int from the specified position.
     *
     * @param pos the position in the memory mapped file
     * @return the value read
     */
    public int getInt(long pos) {
        seek(pos);
        return unsafe.getInt(this.mappedAddr + this.filePosition);
    }

    /**
     * Reads an int (volatile) from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public int getIntVolatile(long pos) {
        seek(pos);
        return unsafe.getIntVolatile(null, this.mappedAddr + this.filePosition);
    }

    /**
     * Reads a long from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLong(long pos) {
        seek(pos);
        return unsafe.getLong(this.mappedAddr + this.filePosition);
    }

    /**
     * Reads a long (volatile) from the specified position.
     *
     * @param pos position in the memory mapped file
     * @return the value read
     */
    public long getLongVolatile(long pos) {
        seek(pos);
        return unsafe.getLongVolatile(null, this.mappedAddr + this.filePosition);
    }

    /**
     * Writes a byte to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putByte(long pos, byte val) {
        seek(pos);
        unsafe.putByte(this.mappedAddr + this.filePosition, val);
    }

    /**
     * Writes a byte (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putByteVolatile(long pos, byte val) {
        seek(pos);
        unsafe.putByteVolatile(null, this.mappedAddr + this.filePosition, val);
    }

    /**
     * Writes an int to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putInt(long pos, int val) {
        seek(pos);
        unsafe.putInt(this.mappedAddr + this.filePosition, val);
    }

    /**
     * Writes an int (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putIntVolatile(long pos, int val) {
        seek(pos);
        unsafe.putIntVolatile(null, this.mappedAddr + this.filePosition, val);
    }

    /**
     * Writes a long to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putLong(long pos, long val) {
        seek(pos);
        unsafe.putLong(this.mappedAddr + this.filePosition, val);
    }

    /**
     * Writes a long (volatile) to the specified position.
     *
     * @param pos the position in the memory mapped file
     * @param val the value to write
     */
    public void putLongVolatile(long pos, long val) {
        seek(pos);
        unsafe.putLongVolatile(null, this.mappedAddr + this.filePosition, val);
    }

    /**
     * Reads a buffer of data.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the input buffer
     * @param offset the offset in the buffer of the first byte to read data into
     * @param length the length of the data
     */
    public void getBytes(long pos, byte[] data, int offset, int length) {
        seek(pos);
        unsafe.copyMemory(null, this.mappedAddr + this.filePosition, data, BYTE_ARRAY_OFFSET + offset, length);
    }

    /**
     * Writes a buffer of data.
     *
     * @param pos    the position in the memory mapped file
     * @param data   the output buffer
     * @param offset the offset in the buffer of the first byte to write
     * @param length the length of the data
     */
    public void setBytes(long pos, byte[] data, int offset, int length) {
        seek(pos);
        unsafe.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, this.mappedAddr + this.filePosition, length);
    }

    public boolean compareAndSwapInt(long pos, int expected, int value) {
        seek(pos);
        return unsafe.compareAndSwapInt(null, this.mappedAddr + this.filePosition, expected, value);
    }

    public boolean compareAndSwapLong(long pos, long expected, long value) {
        seek(pos);
        return unsafe.compareAndSwapLong(null, this.mappedAddr + this.filePosition, expected, value);
    }

    public long getAndAddLong(long pos, long delta) {
        seek(pos);
        return unsafe.getAndAddLong(null, this.mappedAddr + this.filePosition, delta);
    }

    public void flush() {

    }


}
