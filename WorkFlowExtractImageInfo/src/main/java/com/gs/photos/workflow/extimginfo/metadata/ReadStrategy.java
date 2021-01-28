package com.gs.photos.workflow.extimginfo.metadata;

import java.io.IOException;
import java.io.InputStream;

public interface ReadStrategy {
	//
	public int readInt(byte[] buf, int start_idx);

	public int readInt(InputStream is) throws IOException;

	public long readLong(byte[] buf, int start_idx);

	public long readLong(InputStream is) throws IOException;

	public float readS15Fixed16Number(byte[] buf, int start_idx);

	public float readS15Fixed16Number(InputStream is) throws IOException;

	public short readShort(byte[] buf, int start_idx);

	public short readShort(InputStream is) throws IOException;

	public float readU16Fixed16Number(byte[] buf, int start_idx);

	public float readU16Fixed16Number(InputStream is) throws IOException;

	public float readU8Fixed8Number(byte[] buf, int start_idx);

	public float readU8Fixed8Number(InputStream is) throws IOException;

	public long readUnsignedInt(byte[] buf, int start_idx);

	public long readUnsignedInt(InputStream is) throws IOException;

	public int readUnsignedShort(byte[] buf, int start_idx);

	public int readUnsignedShort(InputStream is) throws IOException;
}