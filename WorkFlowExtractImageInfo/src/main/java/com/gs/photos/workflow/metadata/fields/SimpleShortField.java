package com.gs.photos.workflow.metadata.fields;

import java.io.IOException;

import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.Tag;
import com.gs.photos.workflow.metadata.tiff.ShortField;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class SimpleShortField extends SimpleAbstractField<short[]> {

	protected SimpleShortField(int fieldLength, int offset, short type) {
		super(fieldLength, offset, type);
	}

	protected int currentLength;
	protected short[] data;

	@Override
	public TiffField<short[]> createTiffField(Tag tag, short tagValue) {
		TiffField<short[]> shortField = new ShortField(tag, this, tagValue);
		return shortField;
	}

	@Override
	public short[] getData() {
		return data;
	}

	@Override
	public void updateData(FileChannelDataInput rin) {
		short[] sdata = new short[getFieldLength()];
		try {
			if (sdata.length == 1) {
				rin.position(offset);
				sdata[0] = rin.readShort();
				currentLength = 4;
			} else if (sdata.length == 2) {
				rin.position(offset);
				sdata[0] = rin.readShort();
				currentLength += 2;
				rin.position(offset + currentLength);
				sdata[1] = rin.readShort();
				currentLength += 2;
			} else {
				rin.position(offset);
				int toOffset = rin.readInt();
				currentLength += 4;
				for (int j = 0; j < sdata.length; j++) {
					rin.position(toOffset);
					sdata[j] = rin.readShort();
					toOffset += 2;
				}
			}
			data = sdata;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int getNextOffset() {
		// TODO Auto-generated method stub
		return offset + currentLength;
	}

}
