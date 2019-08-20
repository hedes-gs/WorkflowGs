package com.gs.photos.workflow.metadata.fields;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.workflow.model.FieldType;

public class SimpleFieldFactory {

	private static final Map<FieldType, Class<? extends SimpleAbstractField<?>>> fieldTypeToSimpleAbstractField = new HashMap<FieldType, Class<? extends SimpleAbstractField<?>>>() {
		{
			put(FieldType.BYTE, SimpleByteField.class);
			put(FieldType.ASCII, SimpleASCIIField.class);
			put(FieldType.SHORT, SimpleShortField.class);
			put(FieldType.LONG, SimpleLongField.class);
			put(FieldType.RATIONAL, SimpleRationalField.class);
			put(FieldType.SBYTE, SimpleSignedByteField.class);
			put(FieldType.UNDEFINED, SimpleUndefinedField.class);
			put(FieldType.SSHORT, SimpleSignedShortField.class);
			put(FieldType.SLONG, SimpleSignedLongField.class);
			put(FieldType.SRATIONAL, SimpleSignedRationalField.class);
			put(FieldType.FLOAT, SimpleFloatField.class);
			put(FieldType.DOUBLE, SimpleDoubleField.class);
			put(FieldType.IFD, SimpleIFDField.class);
			put(FieldType.WINDOWSXP, SimpleWindowsXPField.class);
			put(FieldType.UNKNOWN, SimpleUnknonwField.class);
		}
	};

	public static SimpleAbstractField<?> createField(short fieldType, int fieldLength, int offset) {
		Class<? extends SimpleAbstractField<?>> cl = fieldTypeToSimpleAbstractField.get(FieldType.fromShort(fieldType));
		try {
			Constructor<? extends SimpleAbstractField<?>> constructor = cl.getDeclaredConstructor(Integer.TYPE,
					Integer.TYPE, Short.TYPE);
			return constructor.newInstance(fieldLength, offset, fieldType);
		} catch (InvocationTargetException | IllegalArgumentException | IllegalAccessException | InstantiationException
				| NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
		return null;
	}
}
