package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.LongField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleLongField extends SimpleAbstractField<int[]> {

	protected int nextOffset;
	protected int[] data;

	public SimpleLongField(int fieldLength, int offset, short type) {
		super(fieldLength, offset, type);
	}

	@Override
	public TiffField<int[]> createTiffField(Tag tag, short tagValue) {
		TiffField<int[]> longField = new LongField(tag, this, tagValue);
		return longField;
	}

	@Override
	public int[] getData() {
		return data;
	}

	@Override
	public void updateData(FileChannelDataInput rin) {
		data = new int[getFieldLength()];
		try {
			if (data.length == 1) {
				rin.position(offset);
				data[0] = rin.readInt();
				nextOffset = 4;
			} else {
				rin.position(offset);
				int toOffset = rin.readInt();
				nextOffset = 4;
				for (int j = 0; j < data.length; j++) {
					rin.position(toOffset);
					data[j] = rin.readInt();
					toOffset += 4;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int getNextOffset() {
		return offset + nextOffset;
	}

}
