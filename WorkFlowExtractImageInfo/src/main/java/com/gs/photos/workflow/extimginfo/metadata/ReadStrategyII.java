package com.gs.photos.workflow.extimginfo.metadata;

import java.io.IOException;
import java.io.InputStream;

public class ReadStrategyII implements ReadStrategy {

	private static final ReadStrategyII instance = new ReadStrategyII();

	public static ReadStrategyII getInstance() {
		return instance;
	}

	private ReadStrategyII() {
	}

	@Override
	public int readInt(byte[] buf, int start_idx) {
		return ((buf[start_idx++] & 0xff) | ((buf[start_idx++] & 0xff) << 8) | ((buf[start_idx++] & 0xff) << 16)
				| ((buf[start_idx++] & 0xff) << 24));
	}

	@Override
	public int readInt(InputStream is) throws IOException {
		byte[] buf = new byte[4];
		IOUtils.readFully(is, buf);

		return (((buf[3] & 0xff) << 24) | ((buf[2] & 0xff) << 16) | ((buf[1] & 0xff) << 8) | (buf[0] & 0xff));
	}

	@Override
	public long readLong(byte[] buf, int start_idx) {
		return ((buf[start_idx++] & 0xffL) | (((buf[start_idx++] & 0xffL) << 8) | ((buf[start_idx++] & 0xffL) << 16)
				| ((buf[start_idx++] & 0xffL) << 24) | ((buf[start_idx++] & 0xffL) << 32)
				| ((buf[start_idx++] & 0xffL) << 40) | ((buf[start_idx++] & 0xffL) << 48)
				| (buf[start_idx] & 0xffL) << 56));
	}

	@Override
	public long readLong(InputStream is) throws IOException {
		byte[] buf = new byte[8];
		IOUtils.readFully(is, buf);

		return (((buf[7] & 0xffL) << 56) | ((buf[6] & 0xffL) << 48) | ((buf[5] & 0xffL) << 40)
				| ((buf[4] & 0xffL) << 32) | ((buf[3] & 0xffL) << 24) | ((buf[2] & 0xffL) << 16)
				| ((buf[1] & 0xffL) << 8) | (buf[0] & 0xffL));
	}

	@Override
	public float readS15Fixed16Number(byte[] buf, int start_idx) {
		short s15 = (short) ((buf[start_idx++] & 0xff) | ((buf[start_idx++] & 0xff) << 8));
		int fixed16 = ((buf[start_idx++] & 0xff) | ((buf[start_idx] & 0xff) << 8));

		return s15 + fixed16 / 65536.0f;
	}

	@Override
	public float readS15Fixed16Number(InputStream is) throws IOException {
		byte[] buf = new byte[4];
		IOUtils.readFully(is, buf);

		short s15 = (short) ((buf[0] & 0xff) | ((buf[1] & 0xff) << 8));
		int fixed16 = ((buf[2] & 0xff) | ((buf[3] & 0xff) << 8));

		return s15 + fixed16 / 65536.0f;
	}

	@Override
	public short readShort(byte[] buf, int start_idx) {
		return (short) ((buf[start_idx++] & 0xff) | ((buf[start_idx] & 0xff) << 8));
	}

	@Override
	public short readShort(InputStream is) throws IOException {
		byte[] buf = new byte[2];
		IOUtils.readFully(is, buf);

		return (short) (((buf[1] & 0xff) << 8) | (buf[0] & 0xff));
	}

	@Override
	public float readU16Fixed16Number(byte[] buf, int start_idx) {
		int u16 = ((buf[start_idx++] & 0xff) | ((buf[start_idx++] & 0xff) << 8));
		int fixed16 = ((buf[start_idx++] & 0xff) | ((buf[start_idx] & 0xff) << 8));

		return u16 + fixed16 / 65536.0f;
	}

	@Override
	public float readU16Fixed16Number(InputStream is) throws IOException {
		byte[] buf = new byte[4];
		IOUtils.readFully(is, buf);

		int u16 = ((buf[0] & 0xff) | ((buf[1] & 0xff) << 8));
		int fixed16 = ((buf[2] & 0xff) | ((buf[3] & 0xff) << 8));

		return u16 + fixed16 / 65536.0f;
	}

	@Override
	public float readU8Fixed8Number(byte[] buf, int start_idx) {
		int u8 = (buf[start_idx++] & 0xff);
		int fixed8 = (buf[start_idx] & 0xff);

		return u8 + fixed8 / 256.0f;
	}

	@Override
	public float readU8Fixed8Number(InputStream is) throws IOException {
		byte[] buf = new byte[2];
		IOUtils.readFully(is, buf);

		int u8 = (buf[0] & 0xff);
		int fixed8 = (buf[1] & 0xff);

		return u8 + fixed8 / 256.0f;
	}

	@Override
	public long readUnsignedInt(byte[] buf, int start_idx) {
		return ((buf[start_idx++] & 0xff) | ((buf[start_idx++] & 0xff) << 8) | ((buf[start_idx++] & 0xff) << 16)
				| ((buf[start_idx++] & 0xff) << 24)) & 0xffffffffL;
	}

	@Override
	public long readUnsignedInt(InputStream is) throws IOException {
		byte[] buf = new byte[4];
		IOUtils.readFully(is, buf);

		return (((buf[3] & 0xff) << 24) | ((buf[2] & 0xff) << 16) | ((buf[1] & 0xff) << 8) | (buf[0] & 0xff))
				& 0xffffffffL;
	}

	@Override
	public int readUnsignedShort(byte[] buf, int start_idx) {
		return ((buf[start_idx++] & 0xff) | ((buf[start_idx] & 0xff) << 8));
	}

	@Override
	public int readUnsignedShort(InputStream is) throws IOException {
		byte[] buf = new byte[2];
		IOUtils.readFully(is, buf);

		return (((buf[1] & 0xff) << 8) | (buf[0] & 0xff));
	}
}