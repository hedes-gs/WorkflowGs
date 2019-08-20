package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.FloatField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleFloatField extends SimpleAbstractField<float[]> {

	protected float[] data;
	protected int currentLength;

	public SimpleFloatField(int fieldLength, int offset, short type) {
		super(fieldLength, offset, type);
	}

	@Override
	public TiffField<float[]> createTiffField(Tag tag, short tagValue) {
		TiffField<float[]> floatField = new FloatField(tag, this, tagValue);
		return floatField;
	}

	@Override
	public float[] getData() {
		return null;
	}

	@Override
	public void updateData(FileChannelDataInput rin) {
		try {
			data = new float[getFieldLength()];
			if (data.length == 1) {
				rin.position(offset);
				data[0] = rin.readFloat();
				currentLength = 4;
			} else {
				rin.position(offset);
				int toOffset = rin.readInt();
				currentLength = 4;
				for (int j = 0; j < data.length; j++) {
					rin.position(toOffset);
					data[j] = rin.readFloat();
					toOffset += 4;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int getNextOffset() {
		return offset + currentLength;
	}

}
