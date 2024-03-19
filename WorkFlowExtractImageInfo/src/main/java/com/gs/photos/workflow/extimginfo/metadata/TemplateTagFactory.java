package com.gs.photos.workflow.extimginfo.metadata;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.exif.Tag;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleAbstractField;
import com.gs.photos.workflow.extimginfo.metadata.fields.SimpleIFDField;

public class TemplateTagFactory {

    static final short                                                EXIF_SUB_IFD                 = (short) 0x8769;
    static final short                                                GPS_SUB_IFD                  = (short) 0x8825;
    static final short                                                EXIF_INTEROPERABILITY_OFFSET = (short) 0xA005;
    static final short                                                SUB_IFDS                     = (short) 0x014A;
    static final short                                                EXIF_PRIVATE_TAGS            = (short) 0xc634;
    static final short                                                MAKER_NOTE                   = (short) 0x927c;
    static final short                                                SONY_FOCUS_INFO              = (short) 0x0020;

    protected static Map<Short, Class<? extends AbstractTemplateTag>> convertTagToTemplate         = new HashMap<>() {
                                                                                                       {
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.EXIF_SUB_IFD,
                                                                                                               ExifTagTagTemplate.class);
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.GPS_SUB_IFD,
                                                                                                               GpsTagTemplate.class);
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.EXIF_INTEROPERABILITY_OFFSET,
                                                                                                               InteropTagTemplate.class);
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.SUB_IFDS,
                                                                                                               SubIfdTemplate.class);
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.MAKER_NOTE,
                                                                                                               MakerNoteTemplate.class);
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.EXIF_PRIVATE_TAGS,
                                                                                                               PrivateIfdTemplate.class);
                                                                                                           this.put(
                                                                                                               TemplateTagFactory.SONY_FOCUS_INFO,
                                                                                                               SonyFocusInfoTemplate.class);
                                                                                                       }
                                                                                                   };

    public static AbstractTemplateTag create(
        Tag ftag,
        IFD parent,
        SimpleAbstractField<?> saf,
        IExifService exifService
    ) {
        Class<? extends AbstractTemplateTag> cl = TemplateTagFactory.convertTagToTemplate.get(ftag.getValue());
        if (cl != null) {
            try {
                Constructor<? extends AbstractTemplateTag> constructor = cl
                    .getDeclaredConstructor(Tag.class, IFD.class, SimpleAbstractField.class, IExifService.class);
                return constructor.newInstance(ftag, parent, saf, exifService);
            } catch (
                InvocationTargetException |
                IllegalArgumentException |
                IllegalAccessException |
                InstantiationException |
                NoSuchMethodException |
                SecurityException e) {
                e.printStackTrace();
            }
        } else if (saf instanceof SimpleIFDField) {
            return new SubIfdTemplate(ftag, parent, (SimpleAbstractField<int[]>) saf, exifService);
        }
        return new DefaultTagTemplate(ftag, parent, exifService);
    }

    public static AbstractTemplateTag create(Tag tag, IExifService exifService) {
        return new DefaultTagTemplate(tag, exifService);
    }

}
