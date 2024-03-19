package com.workflow.model;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteObject extends ToByte<SizeAndJpegContent> {
    @Override
    public default byte[] convert(SizeAndJpegContent p) {
        byte[] retValue = new byte[4 + 4 + 4 + p.getJpegContent().length];
        int offset = Bytes.putInt(retValue, 0, p.getWidth());
        offset = Bytes.putInt(retValue, offset, p.getHeight());
        offset = Bytes.putInt(retValue, offset, p.getJpegContent().length);
        offset = Bytes.putByteBuffer(retValue, offset, ByteBuffer.wrap(p.getJpegContent()));
        return retValue;
    }

    @Override
    default SizeAndJpegContent fromByte(byte[] parameter, int offset, int length) {

        long width = Bytes.toInt(parameter, offset);
        long height = Bytes.toInt(parameter, offset + 4);
        int size = Bytes.toInt(parameter, offset + 8);
        byte[] jpegContent = new byte[size];
        System.arraycopy(parameter, offset + 12, jpegContent, 0, size);
        SizeAndJpegContent retValue = SizeAndJpegContent.builder()
            .withHeight((int) height)
            .withWidth((int) width)
            .withJpegContent(jpegContent)
            .build();
        return retValue;
    }

    @Override
    default SizeAndJpegContent fromByte(byte[] parameter) { return this.fromByte(parameter, 0, parameter.length); }

    @Override
    public default ToByte<SizeAndJpegContent> getInstance() { return new ToByteObject() {}; }

}