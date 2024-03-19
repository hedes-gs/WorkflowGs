package com.workflow.model;

import java.util.HashMap;
import java.util.Map;

public enum FieldType {
    BYTE(
        "Byte", (short) 0x0001
    ), ASCII(
        "ASCII", (short) 0x0002
    ), SHORT(
        "Short", (short) 0x0003
    ), LONG(
        "Long", (short) 0x0004
    ), RATIONAL(
        "Rational", (short) 0x0005
    ), SBYTE(
        "SByte", (short) 0x0006
    ), UNDEFINED(
        "Undefined", (short) 0x0007
    ), SSHORT(
        "SShort", (short) 0x0008
    ), SLONG(
        "SLong", (short) 0x0009
    ), SRATIONAL(
        "SRational", (short) 0x000a
    ), FLOAT(
        "Float", (short) 0x000b
    ), DOUBLE(
        "Double", (short) 0x000c
    ), IFD(
        "IFD", (short) 0x000d
    ),
    // This is actually not a TIFF defined field type, internally it is a TIFF BYTE
    // field
    WINDOWSXP(
        "WindowsXP", (short) 0x000e
    ), SUBDIRECTORY(
        "SubDirectory", (short) 0x000f
    ),

    UNKNOWN(
        "Unknown", (short) 0x0000
    );

    private FieldType(
        String name,
        short value
    ) {
        this.name = name;
        this.value = value;
    }

    public String getName() { return this.name; }

    public short getValue() { return this.value; }

    @Override
    public String toString() { return this.name; }

    private final String                       name;
    private final short                        value;

    private static final Map<Short, FieldType> typeMap = new HashMap<Short, FieldType>();

    static {
        for (FieldType fieldType : FieldType.values()) {
            FieldType.typeMap.put(fieldType.getValue(), fieldType);
        }
    }

    public static FieldType fromShort(short value) {
        FieldType fieldType = FieldType.typeMap.get(value);
        if (fieldType == null) { return UNKNOWN; }
        return fieldType;
    }
}