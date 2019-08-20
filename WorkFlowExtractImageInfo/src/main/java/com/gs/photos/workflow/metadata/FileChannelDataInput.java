package com.gs.photos.workflow.metadata;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class FileChannelDataInput implements DataInput, Closeable {

	protected MappedByteBuffer mappedByteBuffer;
	private ReadStrategy setReadStrategy;
	protected static final int MAPPED_MEMORY_SIZE = 4 * 1024 * 1024;

	public FileChannelDataInput() {

	}

	public void setFileChannel(FileChannel fileChannel) {
		try {
			this.mappedByteBuffer = fileChannel.map(
				MapMode.READ_ONLY,
				0,
				MAPPED_MEMORY_SIZE);
			mappedByteBuffer.load();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void ensureOpen() throws IOException {
		if (mappedByteBuffer == null) {
			throw new IOException("mappedByteBuffer is not opened !!");
		}
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		ensureOpen();
		mappedByteBuffer.get(
			b);
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		ensureOpen();
		mappedByteBuffer.get(
			b,
			off,
			len);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		mappedByteBuffer.position(
			mappedByteBuffer.position() + n);
		return 0;
	}

	@Override
	public boolean readBoolean() throws IOException {
		return mappedByteBuffer.get() == 0;
	}

	@Override
	public byte readByte() throws IOException {
		return mappedByteBuffer.get();
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return (mappedByteBuffer.get()) & 0xff;
	}

	@Override
	public short readShort() throws IOException {
		return mappedByteBuffer.getShort();
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return (mappedByteBuffer.getShort()) & 0xfff;
	}

	@Override
	public char readChar() throws IOException {
		return mappedByteBuffer.getChar();
	}

	@Override
	public int readInt() throws IOException {
		return mappedByteBuffer.getInt();
	}

	@Override
	public long readLong() throws IOException {
		return mappedByteBuffer.getLong();
	}

	@Override
	public float readFloat() throws IOException {
		return mappedByteBuffer.getFloat();
	}

	@Override
	public double readDouble() throws IOException {
		return mappedByteBuffer.getDouble();
	}

	@Override
	public String readLine() throws IOException {
		throw new IllegalAccessError("unimplemented method");
	}

	@Override
	public String readUTF() throws IOException {
		throw new IllegalAccessError("unimplemented method");
	}

	public byte[] peek(int imageMagicNumberLen) throws IOException {
		byte[] retValue = new byte[imageMagicNumberLen];
		readFully(
			retValue);
		return retValue;
	}

	public int position() {
		return mappedByteBuffer.position();
	}

	public void position(int streamHead) {
		mappedByteBuffer.position(
			streamHead);
	}

	public void setReadStrategy(ReadStrategy instance) {
		if (instance == ReadStrategyII.getInstance()) {
			mappedByteBuffer.order(
				java.nio.ByteOrder.LITTLE_ENDIAN);
		} else {
			mappedByteBuffer.order(
				java.nio.ByteOrder.BIG_ENDIAN);
		}
		this.setReadStrategy = instance;
	}

	public short readShortAsBigEndian() throws IOException {
		short retValue = readShort();
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
		if (mappedByteBuffer != null) {
			mappedByteBuffer.clear();
			try {
				Method cleaner = mappedByteBuffer.getClass().getMethod(
					"cleaner");
				cleaner.setAccessible(
					true);
				Method clean = Class.forName(
					"sun.misc.Cleaner").getMethod(
						"clean");
				clean.setAccessible(
					true);
				clean.invoke(
					cleaner.invoke(
						mappedByteBuffer));
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		mappedByteBuffer = null;
	}

}