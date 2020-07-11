package com.gs.photo.workflow.exif;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.PostConstruct;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.workflow.model.dtos.ExifDTO;

public class ExifServiceImpl implements IExifService {

    private static final String               TAG_DEC         = "tag-dec";
    private static final String               TAG_HEX         = "tag-hex";
    private static final String               TYPE            = "type";
    private static final String               KEY             = "key";
    private static final String               IFD             = "ifd";
    private static final String               TAG_DESCRIPTION = "tag-description";
    static Logger                             LOGGER          = LoggerFactory.getLogger(IExifService.class);
    static DecimalFormat                      df              = new DecimalFormat("#,###,###.####");
    protected List<String>                    exifFiles;
    protected Multimap<Short, ExifFileRecord> infosPerExifTag = TreeMultimap.create();

    // header : "Tag-hex", "Tag-dec", "IFD", "Key", "Type", "Tag-description"

    public ExifServiceImpl(List<String> exifFiles) { this.exifFiles = exifFiles; }

    @PostConstruct
    public void init() {
        this.exifFiles.forEach((f) -> {
            try (
                InputStream is = Thread.currentThread()
                    .getContextClassLoader()
                    .getResource(f)
                    .openStream()) {

                CSVParser parser = CSVParser
                    .parse(is, Charset.forName("ISO-8859-1"), CSVFormat.RFC4180.withFirstRecordAsHeader());
                for (CSVRecord csvRecord : parser) {
                    Map<String, String> map = csvRecord.toMap();
                    int tagDec = Integer.parseInt(map.get(ExifServiceImpl.TAG_DEC));
                    if (tagDec >= 32768) {
                        tagDec = tagDec - 65536;
                    }
                    ExifFileRecord efr = ExifFileRecord.builder()
                        .withDescription(map.get(ExifServiceImpl.TAG_DESCRIPTION))
                        .withIfd(map.get(ExifServiceImpl.IFD))
                        .withKey(map.get(ExifServiceImpl.KEY))
                        .withType(
                            FieldType.valueOf(
                                map.get(ExifServiceImpl.TYPE)
                                    .toUpperCase()))
                        .withTagHex(map.get(ExifServiceImpl.TAG_HEX))
                        .withTagDec(tagDec)
                        .build();

                    this.infosPerExifTag.put(this.toTagValue(map.get(ExifServiceImpl.IFD)), efr);
                }
            } catch (IOException e) {
                ExifServiceImpl.LOGGER.info("ERROR ", ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        });
    }

    private @Nullable Short toTagValue(String ifdName) {
        switch (ifdName.toUpperCase()) {
            case "GPSINFO":
                return IExifService.GPS_SUB_IFD;
            case "IOP":
                return IExifService.EXIF_INTEROPERABILITY_OFFSET;
            case "IMAGE":
                return IExifService.IFD_IMAGE;
            case "PHOTO":
                return IExifService.EXIF_SUB_IFD;
            case "SONY1":
                return IExifService.EXIF_SONY;
            case "SUBIFD":
                return IExifService.SUB_IFDS;
        }
        throw new IllegalArgumentException("Unknown ifd " + ifdName);
    }

    @Override
    public Tag getTagFrom(short ifdParent, short tag) {
        Optional<ExifFileRecord> infoDesc = Optional.empty();
        infoDesc = this.infosPerExifTag.get(ifdParent)
            .stream()
            .filter((efr) -> efr.tagDec == tag)
            .findFirst();

        if (infoDesc.isEmpty()) {
            infoDesc = this.infosPerExifTag.get(IExifService.IFD_IMAGE)
                .stream()
                .filter((efr) -> efr.tagDec == tag)
                .findFirst();

        }
        ExifFileRecord efr = infoDesc.orElseThrow(() -> {
            ExifServiceImpl.LOGGER
                .error(" unable to find  tag " + Integer.toHexString(tag) + " not found in " + ifdParent);
            return new IllegalArgumentException(" tag " + Integer.toHexString(tag) + " not found in " + ifdParent);
        });
        ExifRecordTag retValue = ExifRecordTag.builder()
            .withName(efr.getKey())
            .withFieldType(efr.getType())
            .withValue(tag)
            .build();
        return retValue;
    }

    @Override
    public ExifDTO.Builder getExifDTOFrom(
        ExifDTO.Builder builder,
        short ifdParent,
        short tag,
        int[] valueAsInt,
        short[] valueAsShort,
        byte[] valueAsByte
    ) {
        Optional<ExifFileRecord> infoDesc = this.infosPerExifTag.get(ifdParent)
            .stream()
            .filter((efr) -> efr.tagDec == tag)
            .findFirst();
        if (infoDesc.isEmpty()) {
            infoDesc = this.infosPerExifTag.get(IExifService.IFD_IMAGE)
                .stream()
                .filter((efr) -> efr.tagDec == tag)
                .findFirst();

        }
        ExifFileRecord efr = infoDesc.orElseThrow(() -> {
            ExifServiceImpl.LOGGER
                .error(" unable to find  tag " + Integer.toHexString(tag) + " not found in " + ifdParent);
            return new IllegalArgumentException(" tag " + Integer.toHexString(tag) + " not found in " + ifdParent);
        });

        builder.withDescription(efr.getDescription())
            .withDisplayableName(efr.getKey());

        switch (efr.getType()) {
            case SHORT_OR_LONG: {
                if (valueAsShort != null) {
                    builder.withDisplayableValue(Arrays.toString(valueAsShort));
                } else if (valueAsInt != null) {
                    builder.withDisplayableValue(Arrays.toString(valueAsInt));
                }
                break;
            }
            case SBYTE:
            case BYTE: {
                builder.withDisplayableValue(Arrays.toString(valueAsByte));
                break;
            }
            case ASCII:
            case ASCII_XMP: {
                builder.withDisplayableValue(
                    new String(valueAsByte,
                        0,
                        valueAsByte[valueAsByte.length - 1] == 0 ? valueAsByte.length - 1 : valueAsByte.length,
                        Charset.forName("UTF-8")));
                break;
            }
            case SLONG:
            case LONG: {
                builder.withDisplayableValue(Arrays.toString(valueAsInt));
                break;
            }
            case SSHORT:
            case SHORT: {
                builder.withDisplayableValue(Arrays.toString(valueAsShort));
                break;
            }
            case SRATIONAL:
            case RATIONAL: {
                if (valueAsInt.length < 2) { throw new IllegalArgumentException("Input data length is too short"); }
                if (valueAsInt[1] == 0) { throw new ArithmeticException("Divided by zero"); }

                long numerator = valueAsInt[0];
                long denominator = valueAsInt[1];
                numerator = (numerator & 0xffffffffL);
                denominator = (denominator & 0xffffffffL);
                builder.withDisplayableValue(
                    ExifServiceImpl.df.format((1.0 * numerator) / denominator) + ", i.e. : " + numerator + " / "
                        + denominator);
                break;
            }
        }
        return builder;
    }

    @Override

    public String toString(Tag ifdParent, Tag tag, Object data) {
        final short shortValueOfifdParent = ifdParent.getValue();
        return this.toString(shortValueOfifdParent, tag, data);
    }

    @Override
    public String toString(FieldType fieldType, Object data) {
        switch (fieldType) {
            case ASCII: {
                if (data instanceof byte[]) {
                    byte[] valueAsByte = (byte[]) data;
                    return new String(valueAsByte, 0, valueAsByte.length - 1, Charset.forName("UTF-8"));
                } else if (data instanceof String) { return (String) data; }
                break;
            }
            case RATIONAL:
            case SRATIONAL: {
                int[] valueAsInt = (int[]) data;
                if (valueAsInt.length < 2) { throw new IllegalArgumentException("Input data length is too short"); }
                if (valueAsInt[1] == 0) { throw new ArithmeticException("Divided by zero"); }

                long numerator = valueAsInt[0];
                long denominator = valueAsInt[1];
                numerator = (numerator & 0xffffffffL);
                denominator = (denominator & 0xffffffffL);
                return ExifServiceImpl.df.format((1.0 * numerator) / denominator) + " [" + numerator + " / "
                    + denominator + "]";
            }
            default: {
                return "<not processed>";
            }
        }
        return "<not processed>";
    }

    @Override
    public String toString(final short shortValueOfifdParent, Tag tag, Object data) {
        Optional<ExifFileRecord> infoDesc = this.infosPerExifTag.get(shortValueOfifdParent)
            .stream()
            .filter((efr) -> efr.tagDec == tag.getValue())
            .findFirst();
        if (infoDesc.isEmpty()) {
            infoDesc = this.infosPerExifTag.get(IExifService.IFD_IMAGE)
                .stream()
                .filter((efr) -> efr.tagDec == tag.getValue())
                .findFirst();

        }
        ExifFileRecord efr = infoDesc.orElseThrow(() -> {
            ExifServiceImpl.LOGGER.error(
                " unable to find  tag " + Integer.toHexString(tag.getValue()) + " not found in "
                    + shortValueOfifdParent);
            return new IllegalArgumentException(
                " tag " + Integer.toHexString(tag.getValue()) + " not found in " + shortValueOfifdParent);
        });

        switch (efr.getType()) {
            case BYTE:
            case SBYTE: {
                byte[] valueAsByte = (byte[]) data;
                return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                    + Arrays.toString(valueAsByte);
            }
            case ASCII_XMP:
            case ASCII: {
                if (data instanceof byte[]) {
                    byte[] valueAsByte = (byte[]) data;
                    return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                        + new String(valueAsByte, Charset.forName("UTF-8"));
                } else if (data instanceof String) {
                    return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - " + data;
                }
                throw new IllegalArgumentException(" type is " + tag.getFieldType() + " and data is " + data);
            }
            case LONG:
            case SLONG: {
                int[] valueAsInt = (int[]) data;
                return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                    + Arrays.toString(valueAsInt);
            }
            case SHORT:
            case SSHORT: {
                short[] valueAsShort = (short[]) data;
                return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                    + Arrays.toString(valueAsShort);
            }
            case RATIONAL:
            case SRATIONAL: {
                int[] valueAsInt = (int[]) data;
                if (valueAsInt.length < 2) { throw new IllegalArgumentException("Input data length is too short"); }
                if (valueAsInt[1] == 0) { throw new ArithmeticException("Divided by zero"); }

                long numerator = valueAsInt[0];
                long denominator = valueAsInt[1];
                numerator = (numerator & 0xffffffffL);
                denominator = (denominator & 0xffffffffL);
                return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                    + ExifServiceImpl.df.format((1.0 * numerator) / denominator) + "" + numerator + " / " + denominator;
            }
            case UNDEFINED: {
                return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - " + data;
            }
            case SHORT_OR_LONG: {
                if (data instanceof short[]) {
                    short[] valueAsShort = (short[]) data;
                    return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                        + Arrays.toString(valueAsShort);
                } else if (data instanceof int[]) {
                    int[] valueAsInt = (int[]) data;
                    return efr.getKey() + "-" + efr.getTagHex() + " - " + efr.getType() + " - "
                        + Arrays.toString(valueAsInt);
                }
                throw new IllegalArgumentException(" type is " + tag.getFieldType() + " and data is " + data);
            }

            default:
                throw new IllegalArgumentException(" unknown type " + tag.getFieldType());
        }
    }

}
