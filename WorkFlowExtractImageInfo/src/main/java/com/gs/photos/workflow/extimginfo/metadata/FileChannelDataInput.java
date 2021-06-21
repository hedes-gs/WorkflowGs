package com.gs.photos.workflow.extimginfo.metadata;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileChannelDataInput implements DataInput, Closeable {

    protected Logger           LOGGER             = LoggerFactory.getLogger(FileChannelDataInput.class);
    protected ByteBuffer       mappedByteBuffer;
    private ReadStrategy       setReadStrategy;
    protected static final int MAPPED_MEMORY_SIZE = 4 * 1024 * 1024;

    public FileChannelDataInput(byte[] content) { this.mappedByteBuffer = ByteBuffer.wrap(content); }

    public void setFileChannel(FileChannel fileChannel) {
        try {
            this.mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, FileChannelDataInput.MAPPED_MEMORY_SIZE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void ensureOpen() throws IOException {
        if (this.mappedByteBuffer == null) { throw new IOException("mappedByteBuffer is not opened !!"); }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        this.ensureOpen();
        try {
            this.mappedByteBuffer.get(b);
        } catch (Exception e) {
            this.LOGGER.error(
                "Error in read fully mapped buffer is {}, length is {} , error is {}",
                this.mappedByteBuffer,
                b != null ? b.length : "<Null buffer>",
                ExceptionUtils.getStackTrace(e));
            throw e;
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        this.ensureOpen();
        this.mappedByteBuffer.get(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        this.mappedByteBuffer.position(this.mappedByteBuffer.position() + n);
        return 0;
    }

    @Override
    public boolean readBoolean() throws IOException { return this.mappedByteBuffer.get() == 0; }

    @Override
    public byte readByte() throws IOException { return this.mappedByteBuffer.get(); }

    @Override
    public int readUnsignedByte() throws IOException { return (this.mappedByteBuffer.get()) & 0xff; }

    @Override
    public short readShort() throws IOException { return this.mappedByteBuffer.getShort(); }

    @Override
    public int readUnsignedShort() throws IOException { return (this.mappedByteBuffer.getShort()) & 0xfff; }

    @Override
    public char readChar() throws IOException { return this.mappedByteBuffer.getChar(); }

    @Override
    public int readInt() throws IOException { return this.mappedByteBuffer.getInt(); }

    @Override
    public long readLong() throws IOException { return this.mappedByteBuffer.getLong(); }

    @Override
    public float readFloat() throws IOException { return this.mappedByteBuffer.getFloat(); }

    @Override
    public double readDouble() throws IOException { return this.mappedByteBuffer.getDouble(); }

    @Override
    public String readLine() throws IOException { throw new IllegalAccessError("unimplemented method"); }

    @Override
    public String readUTF() throws IOException { throw new IllegalAccessError("unimplemented method"); }

    public byte[] peek(int imageMagicNumberLen) throws IOException {
        byte[] retValue = new byte[imageMagicNumberLen];
        this.readFully(retValue);
        return retValue;
    }

    public int position() { return this.mappedByteBuffer.position(); }

    public void position(int streamHead) { this.mappedByteBuffer.position(streamHead); }

    public void setReadStrategy(ReadStrategy instance) {
        if (instance == ReadStrategyII.getInstance()) {
            this.mappedByteBuffer.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        } else {
            this.mappedByteBuffer.order(java.nio.ByteOrder.BIG_ENDIAN);
        }
        this.setReadStrategy = instance;
    }

    public short readShortAsBigEndian() throws IOException {
        short retValue = this.readShort();
        if (this.setReadStrategy == ReadStrategyII.getInstance()) {
            int unsignedValue = retValue & 0x0000ffff;
            int msb = ((unsignedValue >> 8) & 0x000000ff);
            int lsb = unsignedValue & 0x000000ff;
            return (short) (((lsb << 8) & 0xff00) | msb);
        }
        return retValue;
    }

    @Override
    public void close() throws IOException {
        if (this.mappedByteBuffer != null) {
            this.mappedByteBuffer.clear();
        }

        this.mappedByteBuffer = null;
    }

}