package com.gs.photos.workflow.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.gs.photos.workflow.metadata.exif.ExifTag;
import com.gs.photos.workflow.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.metadata.fields.SimpleIFDField;
import com.gs.photos.workflow.metadata.tiff.TiffTag;

public class TemplateTagFactory {

	protected static Map<Tag, Class<? extends AbstractTemplateTag>> convertTagToTemplate = new HashMap<Tag, Class<? extends AbstractTemplateTag>>() {
		{
			put(TiffTag.EXIF_SUB_IFD, ExifTagTagTemplate.class);
			put(TiffTag.GPS_SUB_IFD, GpsTagTemplate.class);
			put(ExifTag.EXIF_INTEROPERABILITY_OFFSET, InteropTagTemplate.class);
			put(TiffTag.SUB_IFDS, SubIfdTemplate.class);
			// put(TiffTag.EXIF_PRIVATE_TAGS, ExifTagTagTemplate.class);

		}
	};

	public static AbstractTemplateTag create(Tag ftag, IFD parent, SimpleAbstractField<?> saf) {
		Class<? extends AbstractTemplateTag> cl = convertTagToTemplate.get(ftag);
		if (cl != null) {
			try {
				Constructor<? extends AbstractTemplateTag> constructor = cl.getDeclaredConstructor(Tag.class, IFD.class,
						SimpleAbstractField.class);
				return constructor.newInstance(ftag, parent, saf);
			} catch (InvocationTargetException | IllegalArgumentException | IllegalAccessException
					| InstantiationException | NoSuchMethodException | SecurityException e) {
				e.printStackTrace();
			}
		} else if (saf instanceof SimpleIFDField) {
			return new SubIfdTemplate(ftag, parent, (SimpleAbstractField<int[]>) saf);
		}
		return new DefaultTagTemplate(ftag, parent);
	}

	public static AbstractTemplateTag create() {
		return new DefaultTagTemplate(null);
	}

}
