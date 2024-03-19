package com.gs.photos.workflow.extimginfo.metadata.fields;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.workflow.model.FieldType;

public class SimpleFieldFactory {

    private static final Map<FieldType, Class<? extends SimpleAbstractField<?>>> fieldTypeToSimpleAbstractField = new HashMap<FieldType, Class<? extends SimpleAbstractField<?>>>() {
        {
            this.put(FieldType.BYTE, SimpleByteField.class);
            this.put(FieldType.ASCII, SimpleASCIIField.class);
            this.put(FieldType.SHORT, SimpleShortField.class);
            this.put(FieldType.LONG, SimpleLongField.class);
            this.put(FieldType.RATIONAL, SimpleRationalField.class);
            this.put(FieldType.SBYTE, SimpleSignedByteField.class);
            this.put(FieldType.UNDEFINED, SimpleUndefinedField.class);
            this.put(FieldType.SSHORT, SimpleSignedShortField.class);
            this.put(FieldType.SLONG, SimpleSignedLongField.class);
            this.put(FieldType.SRATIONAL, SimpleSignedRationalField.class);
            this.put(FieldType.FLOAT, SimpleFloatField.class);
            this.put(FieldType.DOUBLE, SimpleDoubleField.class);
            this.put(FieldType.IFD, SimpleIFDField.class);
            this.put(FieldType.WINDOWSXP, SimpleWindowsXPField.class);
            this.put(FieldType.UNKNOWN, SimpleUnknonwField.class);
            this.put(FieldType.SUBDIRECTORY, SimpleSubDirectoryField.class);
        }
    };

    public static SimpleAbstractField<?> createField(short fieldType, int fieldLength, int offset) {
        Class<? extends SimpleAbstractField<?>> cl = SimpleFieldFactory.fieldTypeToSimpleAbstractField
            .get(FieldType.fromShort(fieldType));
        try {
            Constructor<? extends SimpleAbstractField<?>> constructor = cl
                .getDeclaredConstructor(Integer.TYPE, Integer.TYPE, Short.TYPE);
            return constructor.newInstance(fieldLength, offset, fieldType);
        } catch (
            InvocationTargetException |
            IllegalArgumentException |
            IllegalAccessException |
            InstantiationException |
            NoSuchMethodException |
            SecurityException e) {
            e.printStackTrace();
        }
        return null;
    }
}
