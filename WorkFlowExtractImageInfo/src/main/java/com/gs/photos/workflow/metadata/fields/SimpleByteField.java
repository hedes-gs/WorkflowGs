package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.ByteField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleByteField extends SimpleAbstractField<byte[]> {

	protected byte[] data;
	protected int currentLength;

	public SimpleByteField(int fieldLength, int offset, short type) {
		super(fieldLength, offset, type);
	}

	@Override
	public TiffField<byte[]> createTiffField(Tag tag, short tagValue) {
		TiffField<byte[]> byteField = new ByteField(tag, this, tagValue);
		return byteField;
	}

	@Override
	public byte[] getData() {
		// TODO Auto-generated method stub
		return data;
	}

	@Override
	public void updateData(FileChannelDataInput rin) {
		try {
			data = new byte[getFieldLength()];
			rin.position(offset);
			if (data.length <= 4) {
				rin.readFully(data, 0, data.length);
			} else {
				rin.position(rin.readInt());
				rin.readFully(data, 0, data.length);
			}
			currentLength = 4;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int getNextOffset() {
		return offset + currentLength;
	}

}
