package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.DoubleField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleDoubleField extends SimpleAbstractField<double[]> {

	protected double[] data;
	protected int currentLength;

	public SimpleDoubleField(int fieldLength, int offset, short type) {
		super(fieldLength, offset, type);
	}

	@Override
	public TiffField<double[]> createTiffField(Tag tag, short tagValue) {
		TiffField<double[]> doubleField = new DoubleField(tag, this, tagValue);
		return doubleField;
	}

	@Override
	public double[] getData() {
		return data;
	}

	@Override
	public void updateData(FileChannelDataInput rin) {
		try {
			data = new double[getFieldLength()];
			rin.position(offset);
			int toOffset = rin.readInt();
			currentLength = 4;
			for (int j = 0; j < data.length; j++) {
				rin.position(toOffset);
				data[j] = rin.readDouble();
				toOffset += 8;
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
