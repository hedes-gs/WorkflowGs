package com.workflow.model;

import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.util.Bytes;

public interface ToByteSetOfStrings extends ToByte<Set<String>> {

    @Override
    public default byte[] convert(Set<String> p) {
        List<byte[]> listOfStrings = p.stream()
            .map((s) -> {
                byte[] stringAsbyte = s.getBytes(Charset.forName("UTF-8"));
                byte[] retValue = new byte[stringAsbyte.length + Bytes.SIZEOF_INT];
                Bytes.putInt(retValue, 0, stringAsbyte.length);
                System.arraycopy(stringAsbyte, 0, retValue, Bytes.SIZEOF_INT, retValue.length);
                return retValue;
            })
            .collect(Collectors.toList());

        int totalSize = listOfStrings.stream()
            .mapToInt((s) -> s.length)
            .sum();
        byte[] retValue = new byte[totalSize + Bytes.SIZEOF_INT];
        int offset = Bytes.putInt(retValue, 0, totalSize);

        for (byte[] b : listOfStrings) {
            System.arraycopy(b, 0, retValue, offset, b.length);
            offset = offset + b.length;
        }
        return retValue;
    }

    @Override
    public default Set<String> fromByte(byte[] parameter, int offset, int length) {
        Set<String> retValue = new HashSet<>();
        int currentOffset = offset;
        int nbOfElements = Bytes.readAsInt(parameter, currentOffset, Bytes.SIZEOF_INT);
        currentOffset = currentOffset + Bytes.SIZEOF_INT;
        for (int k = 0; k < nbOfElements; k++) {
            int currentLength = Bytes.readAsInt(parameter, currentOffset, Bytes.SIZEOF_INT);
            currentOffset = currentOffset + Bytes.SIZEOF_INT;
            String currentString = new String(parameter, currentOffset, currentLength, Charset.forName("UTF-8"));
            retValue.add(currentString);
        }

        return retValue;
    }

    @Override
    public default Set<String> fromByte(byte[] parameter) {
        Set<String> retValue = new HashSet<>();
        int currentOffset = 0;
        int nbOfElements = Bytes.readAsInt(parameter, currentOffset, Bytes.SIZEOF_INT);
        currentOffset = currentOffset + Bytes.SIZEOF_INT;
        for (int k = 0; k < nbOfElements; k++) {
            int currentLength = Bytes.readAsInt(parameter, currentOffset, Bytes.SIZEOF_INT);
            currentOffset = currentOffset + Bytes.SIZEOF_INT;
            String currentString = new String(parameter, currentOffset, currentLength, Charset.forName("UTF-8"));
            retValue.add(currentString);
        }

        return retValue;
    }

    @Override
    public default ToByte<Set<String>> getInstance() { return new ToByteSetOfStrings() {}; }

}
