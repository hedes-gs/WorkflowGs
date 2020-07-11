package com.workflow.model;

import java.nio.charset.Charset;

public interface ToByteString extends ToByte<String> {

    @Override
    public default byte[] convert(String p) { return p.getBytes(); }

    @Override
    public default String fromByte(byte[] parameter, int offset, int length) {
        return new String(parameter, offset, length).trim();
    }

    @Override
    public default String fromByte(byte[] parameter) { return new String(parameter, Charset.forName("UTF-8")).trim(); }

    @Override
    public default ToByte<String> getInstance() { return new ToByteString() {}; }

}
