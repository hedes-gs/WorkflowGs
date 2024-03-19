package com.gs.photo.common.workflow.exif;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
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

    private static final String                TAG_DEC         = "tag-dec";
    private static final String                TAG_HEX         = "tag-hex";
    private static final String                TYPE            = "type";
    private static final String                KEY             = "key";
    private static final String                IFD             = "ifd";
    private static final String                TAG_DESCRIPTION = "tag-description";
    static Logger                              LOGGER          = LoggerFactory.getLogger(IExifService.class);
    static DecimalFormat                       df              = new DecimalFormat("#,###,###.####");
    protected List<String>                     exifFiles;
    protected static final Map<String, String> SONY_OBJECTIFS  = new HashMap<>();
    protected Multimap<Short, ExifFileRecord>  infosPerExifTag = TreeMultimap.create();

    // header : "Tag-hex", "Tag-dec", "IFD", "Key", "Type", "Tag-description"

    static {
        ExifServiceImpl.SONY_OBJECTIFS.put("0", "Minolta AF 28-85mm F3.5-4.5 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("1", "Minolta AF 80-200mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("2", "Minolta AF 28-70mm F2.8 G");
        ExifServiceImpl.SONY_OBJECTIFS.put("3", "Minolta AF 28-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("4", "Minolta AF 85mm F1.4G");
        ExifServiceImpl.SONY_OBJECTIFS.put("5", "Minolta AF 35-70mm F3.5-4.5 [II]");
        ExifServiceImpl.SONY_OBJECTIFS.put("6", "Minolta AF 24-85mm F3.5-4.5 [New]");
        ExifServiceImpl.SONY_OBJECTIFS.put("7", "Minolta AF 100-300mm F4.5-5.6 APO [New] or 100-400mm or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("7.1", "Minolta AF 100-400mm F4.5-6.7 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("7.2", "Sigma AF 100-300mm F4 EX DG IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("8", "Minolta AF 70-210mm F4.5-5.6 [II]");
        ExifServiceImpl.SONY_OBJECTIFS.put("9", "Minolta AF 50mm F3.5 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("10", "Minolta AF 28-105mm F3.5-4.5 [New]");
        ExifServiceImpl.SONY_OBJECTIFS.put("11", "Minolta AF 300mm F4 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("12", "Minolta AF 100mm F2.8 Soft Focus");
        ExifServiceImpl.SONY_OBJECTIFS.put("13", "Minolta AF 75-300mm F4.5-5.6 (New or II)");
        ExifServiceImpl.SONY_OBJECTIFS.put("14", "Minolta AF 100-400mm F4.5-6.7 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("15", "Minolta AF 400mm F4.5 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("16", "Minolta AF 17-35mm F3.5 G");
        ExifServiceImpl.SONY_OBJECTIFS.put("17", "Minolta AF 20-35mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("18", "Minolta AF 28-80mm F3.5-5.6 II");
        ExifServiceImpl.SONY_OBJECTIFS.put("19", "Minolta AF 35mm F1.4 G");
        ExifServiceImpl.SONY_OBJECTIFS.put("20", "Minolta/Sony 135mm F2.8 [T4.5] STF");
        ExifServiceImpl.SONY_OBJECTIFS.put("22", "Minolta AF 35-80mm F4-5.6 II");
        ExifServiceImpl.SONY_OBJECTIFS.put("23", "Minolta AF 200mm F4 Macro APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("24", "Minolta/Sony AF 24-105mm F3.5-4.5 (D) or Sigma or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.1", "Sigma 18-50mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.2", "Sigma 17-70mm F2.8-4.5 DC Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.3", "Sigma 20-40mm F2.8 EX DG Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.4", "Sigma 18-200mm F3.5-6.3 DC");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.5", "Sigma DC 18-125mm F4-5,6 D");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.6", "Tamron SP AF 28-75mm F2.8 XR Di LD Aspherical [IF] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("24.7", "Sigma 15-30mm F3.5-4.5 EX DG Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("25", "Minolta AF 100-300mm F4.5-5.6 APO (D) or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25.1", "Sigma 100-300mm F4 EX (APO (D) or D IF)");
        ExifServiceImpl.SONY_OBJECTIFS.put("25.2", "Sigma 70mm F2.8 EX DG Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25.3", "Sigma 20mm F1.8 EX DG Aspherical RF");
        ExifServiceImpl.SONY_OBJECTIFS.put("25.4", "Sigma 30mm F1.4 EX DC");
        ExifServiceImpl.SONY_OBJECTIFS.put("25.5", "Sigma 24mm F1.8 EX DG ASP Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("27", "Minolta AF 85mm F1.4 G (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("28", "Minolta/Sony AF 100mm F2.8 Macro (D) or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("28.1", "Tamron SP AF 90mm F2.8 Di Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("28.2", "Tamron SP AF 180mm F3.5 Di LD [IF] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("29", "Minolta/Sony AF 75-300mm F4.5-5.6 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("30", "Minolta AF 28-80mm F3.5-5.6 (D) or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("30.1", "Sigma AF 10-20mm F4-5.6 EX DC");
        ExifServiceImpl.SONY_OBJECTIFS.put("30.2", "Sigma AF 12-24mm F4.5-5.6 EX DG");
        ExifServiceImpl.SONY_OBJECTIFS.put("30.3", "Sigma 28-70mm EX DG F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("30.4", "Sigma 55-200mm F4-5.6 DC");
        ExifServiceImpl.SONY_OBJECTIFS.put("31", "Minolta/Sony AF 50mm F2.8 Macro (D) or F3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("31.1", "Minolta/Sony AF 50mm F3.5 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("32", "Minolta/Sony AF 300mm F2.8 G or 1.5x Teleconverter");
        ExifServiceImpl.SONY_OBJECTIFS.put("33", "Minolta/Sony AF 70-200mm F2.8 G");
        ExifServiceImpl.SONY_OBJECTIFS.put("35", "Minolta AF 85mm F1.4 G (D) Limited");
        ExifServiceImpl.SONY_OBJECTIFS.put("36", "Minolta AF 28-100mm F3.5-5.6 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("38", "Minolta AF 17-35mm F2.8-4 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("39", "Minolta AF 28-75mm F2.8 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("40", "Minolta/Sony AF DT 18-70mm F3.5-5.6 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("41", "Minolta/Sony AF DT 11-18mm F4.5-5.6 (D) or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("41.1", "Tamron SP AF 11-18mm F4.5-5.6 Di II LD Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("42", "Minolta/Sony AF DT 18-200mm F3.5-6.3 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("43", "Sony 35mm F1.4 G (SAL35F14G)");
        ExifServiceImpl.SONY_OBJECTIFS.put("44", "Sony 50mm F1.4 (SAL50F14)");
        ExifServiceImpl.SONY_OBJECTIFS.put("45", "Carl Zeiss Planar T* 85mm F1.4 ZA (SAL85F14Z)");
        ExifServiceImpl.SONY_OBJECTIFS.put("46", "Carl Zeiss Vario-Sonnar T* DT 16-80mm F3.5-4.5 ZA (SAL1680Z)");
        ExifServiceImpl.SONY_OBJECTIFS.put("47", "Carl Zeiss Sonnar T* 135mm F1.8 ZA (SAL135F18Z)");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("48", "Carl Zeiss Vario-Sonnar T* 24-70mm F2.8 ZA SSM (SAL2470Z) or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("48.1", "Carl Zeiss Vario-Sonnar T* 24-70mm F2.8 ZA SSM II (SAL2470Z2)");
        ExifServiceImpl.SONY_OBJECTIFS.put("48.2", "Tamron SP 24-70mm F2.8 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("49", "Sony DT 55-200mm F4-5.6 (SAL55200)");
        ExifServiceImpl.SONY_OBJECTIFS.put("50", "Sony DT 18-250mm F3.5-6.3 (SAL18250)");
        ExifServiceImpl.SONY_OBJECTIFS.put("51", "Sony DT 16-105mm F3.5-5.6 (SAL16105)");
        ExifServiceImpl.SONY_OBJECTIFS.put("52", "Sony 70-300mm F4.5-5.6 G SSM (SAL70300G) or G SSM II or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("52.1", "Sony 70-300mm F4.5-5.6 G SSM II (SAL70300G2)");
        ExifServiceImpl.SONY_OBJECTIFS.put("52.2", "Tamron SP 70-300mm F4-5.6 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("53", "Sony 70-400mm F4-5.6 G SSM (SAL70400G)");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("54", "Carl Zeiss Vario-Sonnar T* 16-35mm F2.8 ZA SSM (SAL1635Z) or ZA SSM II");
        ExifServiceImpl.SONY_OBJECTIFS.put("54.1", "Carl Zeiss Vario-Sonnar T* 16-35mm F2.8 ZA SSM II (SAL1635Z2)");
        ExifServiceImpl.SONY_OBJECTIFS.put("55", "Sony DT 18-55mm F3.5-5.6 SAM (SAL1855) or SAM II");
        ExifServiceImpl.SONY_OBJECTIFS.put("55.1", "Sony DT 18-55mm F3.5-5.6 SAM II (SAL18552)");
        ExifServiceImpl.SONY_OBJECTIFS.put("56", "Sony DT 55-200mm F4-5.6 SAM (SAL55200-2)");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("57", "Sony DT 50mm F1.8 SAM (SAL50F18) or Tamron Lens or Commlite CM-EF-NEX adapter");
        ExifServiceImpl.SONY_OBJECTIFS.put("57.1", "Tamron SP AF 60mm F2 Di II LD [IF] Macro 1:1");
        ExifServiceImpl.SONY_OBJECTIFS.put("57.2", "Tamron 18-270mm F3.5-6.3 Di II PZD");
        ExifServiceImpl.SONY_OBJECTIFS.put("58", "Sony DT 30mm F2.8 Macro SAM (SAL30M28)");
        ExifServiceImpl.SONY_OBJECTIFS.put("59", "Sony 28-75mm F2.8 SAM (SAL2875)");
        ExifServiceImpl.SONY_OBJECTIFS.put("60", "Carl Zeiss Distagon T* 24mm F2 ZA SSM (SAL24F20Z)");
        ExifServiceImpl.SONY_OBJECTIFS.put("61", "Sony 85mm F2.8 SAM (SAL85F28)");
        ExifServiceImpl.SONY_OBJECTIFS.put("62", "Sony DT 35mm F1.8 SAM (SAL35F18)");
        ExifServiceImpl.SONY_OBJECTIFS.put("63", "Sony DT 16-50mm F2.8 SSM (SAL1650)");
        ExifServiceImpl.SONY_OBJECTIFS.put("64", "Sony 500mm F4 G SSM (SAL500F40G)");
        ExifServiceImpl.SONY_OBJECTIFS.put("65", "Sony DT 18-135mm F3.5-5.6 SAM (SAL18135)");
        ExifServiceImpl.SONY_OBJECTIFS.put("66", "Sony 300mm F2.8 G SSM II (SAL300F28G2)");
        ExifServiceImpl.SONY_OBJECTIFS.put("67", "Sony 70-200mm F2.8 G SSM II (SAL70200G2)");
        ExifServiceImpl.SONY_OBJECTIFS.put("68", "Sony DT 55-300mm F4.5-5.6 SAM (SAL55300)");
        ExifServiceImpl.SONY_OBJECTIFS.put("69", "Sony 70-400mm F4-5.6 G SSM II (SAL70400G2)");
        ExifServiceImpl.SONY_OBJECTIFS.put("70", "Carl Zeiss Planar T* 50mm F1.4 ZA SSM (SAL50F14Z)");
        ExifServiceImpl.SONY_OBJECTIFS.put("128", "Tamron or Sigma Lens (128)");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.1", "Tamron AF 18-200mm F3.5-6.3 XR Di II LD Aspherical [IF] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.2", "Tamron AF 28-300mm F3.5-6.3 XR Di LD Aspherical [IF] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.3", "Tamron AF 28-200mm F3.8-5.6 XR Di Aspherical [IF] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.4", "Tamron SP AF 17-35mm F2.8-4 Di LD Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.5", "Sigma AF 50-150mm F2.8 EX DC APO HSM II");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.6", "Sigma 10-20mm F3.5 EX DC HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.7", "Sigma 70-200mm F2.8 II EX DG APO MACRO HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.8", "Sigma 10mm F2.8 EX DC HSM Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.9", "Sigma 50mm F1.4 EX DG HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.10", "Sigma 85mm F1.4 EX DG HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.11", "Sigma 24-70mm F2.8 IF EX DG HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.12", "Sigma 18-250mm F3.5-6.3 DC OS HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.13", "Sigma 17-50mm F2.8 EX DC HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.14", "Sigma 17-70mm F2.8-4 DC Macro HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.15", "Sigma 150mm F2.8 EX DG OS HSM APO Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.16", "Sigma 150-500mm F5-6.3 APO DG OS HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.17", "Tamron AF 28-105mm F4-5.6 [IF]");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.18", "Sigma 35mm F1.4 DG HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.19", "Sigma 18-35mm F1.8 DC HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.20", "Sigma 50-500mm F4.5-6.3 APO DG OS HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.21", "Sigma 24-105mm F4 DG HSM | A");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.22", "Sigma 30mm F1.4");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.23", "Sigma 35mm F1.4 DG HSM | A");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.24", "Sigma 105mm F2.8 EX DG OS HSM Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.25", "Sigma 180mm F2.8 EX DG OS HSM APO Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.26", "Sigma 18-300mm F3.5-6.3 DC Macro HSM | C");
        ExifServiceImpl.SONY_OBJECTIFS.put("128.27", "Sigma 18-50mm F2.8-4.5 DC HSM");
        ExifServiceImpl.SONY_OBJECTIFS.put("129", "Tamron Lens (129)");
        ExifServiceImpl.SONY_OBJECTIFS.put("129.1", "Tamron 200-400mm F5.6 LD");
        ExifServiceImpl.SONY_OBJECTIFS.put("129.2", "Tamron 70-300mm F4-5.6 LD");
        ExifServiceImpl.SONY_OBJECTIFS.put("131", "Tamron 20-40mm F2.7-3.5 SP Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("135", "Vivitar 28-210mm F3.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("136", "Tokina EMZ M100 AF 100mm F3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("137", "Cosina 70-210mm F2.8-4 AF");
        ExifServiceImpl.SONY_OBJECTIFS.put("138", "Soligor 19-35mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("139", "Tokina AF 28-300mm F4-6.3");
        ExifServiceImpl.SONY_OBJECTIFS.put("142", "Cosina AF 70-300mm F4.5-5.6 MC");
        ExifServiceImpl.SONY_OBJECTIFS.put("146", "Voigtlander Macro APO-Lanthar 125mm F2.5 SL");
        ExifServiceImpl.SONY_OBJECTIFS.put("194", "Tamron SP AF 17-50mm F2.8 XR Di II LD Aspherical [IF]");
        ExifServiceImpl.SONY_OBJECTIFS.put("202", "Tamron SP AF 70-200mm F2.8 Di LD [IF] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("203", "Tamron SP 70-200mm F2.8 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("204", "Tamron SP 24-70mm F2.8 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("212", "Tamron 28-300mm F3.5-6.3 Di PZD");
        ExifServiceImpl.SONY_OBJECTIFS.put("213", "Tamron 16-300mm F3.5-6.3 Di II PZD Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("214", "Tamron SP 150-600mm F5-6.3 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("215", "Tamron SP 15-30mm F2.8 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("216", "Tamron SP 45mm F1.8 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("217", "Tamron SP 35mm F1.8 Di USD");
        ExifServiceImpl.SONY_OBJECTIFS.put("218", "Tamron SP 90mm F2.8 Di Macro 1:1 USD (F017)");
        ExifServiceImpl.SONY_OBJECTIFS.put("220", "Tamron SP 150-600mm F5-6.3 Di USD G2");
        ExifServiceImpl.SONY_OBJECTIFS.put("224", "Tamron SP 90mm F2.8 Di Macro 1:1 USD (F004)");
        ExifServiceImpl.SONY_OBJECTIFS.put("255", "Tamron Lens (255)");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.1", "Tamron SP AF 17-50mm F2.8 XR Di II LD Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.2", "Tamron AF 18-250mm F3.5-6.3 XR Di II LD");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.3", "Tamron AF 55-200mm F4-5.6 Di II LD Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.4", "Tamron AF 70-300mm F4-5.6 Di LD Macro 1:2");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.5", "Tamron SP AF 200-500mm F5.0-6.3 Di LD IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.6", "Tamron SP AF 10-24mm F3.5-4.5 Di II LD Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.7", "Tamron SP AF 70-200mm F2.8 Di LD IF Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.8", "Tamron SP AF 28-75mm F2.8 XR Di LD Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("255.9", "Tamron AF 90-300mm F4.5-5.6 Telemacro");
        ExifServiceImpl.SONY_OBJECTIFS.put("1868", "Sigma MC-11 SA-E Mount Converter with not-supported Sigma lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2550", "Minolta AF 50mm F1.7");
        ExifServiceImpl.SONY_OBJECTIFS.put("2551", "Minolta AF 35-70mm F4 or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2551.1", "Sigma UC AF 28-70mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2551.2", "Sigma AF 28-70mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("2551.3", "Sigma M-AF 70-200mm F2.8 EX Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("2551.4", "Quantaray M-AF 35-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2551.5", "Tokina 28-70mm F2.8-4.5 AF");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552", "Minolta AF 28-85mm F3.5-4.5 or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.1", "Tokina 19-35mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.2", "Tokina 28-70mm F2.8 AT-X");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.3", "Tokina 80-400mm F4.5-5.6 AT-X AF II 840");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.4", "Tokina AF PRO 28-80mm F2.8 AT-X 280");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.5", "Tokina AT-X PRO [II] AF 28-70mm F2.6-2.8 270");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.6", "Tamron AF 19-35mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.7", "Angenieux AF 28-70mm F2.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.8", "Tokina AT-X 17 AF 17mm F3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2552.9", "Tokina 20-35mm F3.5-4.5 II AF");
        ExifServiceImpl.SONY_OBJECTIFS.put("2553", "Minolta AF 28-135mm F4-4.5 or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2553.1", "Sigma ZOOM-alpha 35-135mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2553.2", "Sigma 28-105mm F2.8-4 Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("2553.3", "Sigma 28-105mm F4-5.6 UC");
        ExifServiceImpl.SONY_OBJECTIFS.put("2553.4", "Tokina AT-X 242 AF 24-200mm F3.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2554", "Minolta AF 35-105mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2555", "Minolta AF 70-210mm F4 Macro or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2555.1", "Sigma 70-210mm F4-5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2555.2", "Sigma M-AF 70-200mm F2.8 EX APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2555.3", "Sigma 75-200mm F2.8-3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2556", "Minolta AF 135mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("2557", "Minolta/Sony AF 28mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("2558", "Minolta AF 24-50mm F4");
        ExifServiceImpl.SONY_OBJECTIFS.put("2560", "Minolta AF 100-200mm F4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561", "Minolta AF 75-300mm F4.5-5.6 or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.1", "Sigma 70-300mm F4-5.6 DL Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.2", "Sigma 300mm F4 APO Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.3", "Sigma AF 500mm F4.5 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.4", "Sigma AF 170-500mm F5-6.3 APO Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.5", "Tokina AT-X AF 300mm F4");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.6", "Tokina AT-X AF 400mm F5.6 SD");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.7", "Tokina AF 730 II 75-300mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.8", "Sigma 800mm F5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.9", "Sigma AF 400mm F5.6 APO Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2561.10", "Sigma 1000mm F8 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2562", "Minolta AF 50mm F1.4 [New]");
        ExifServiceImpl.SONY_OBJECTIFS.put("2563", "Minolta AF 300mm F2.8 APO or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2563.1", "Sigma AF 50-500mm F4-6.3 EX DG APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2563.2", "Sigma AF 170-500mm F5-6.3 APO Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("2563.3", "Sigma AF 500mm F4.5 EX DG APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2563.4", "Sigma 400mm F5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2564", "Minolta AF 50mm F2.8 Macro or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2564.1", "Sigma 50mm F2.8 EX Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2565", "Minolta AF 600mm F4 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2566", "Minolta AF 24mm F2.8 or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2566.1", "Sigma 17-35mm F2.8-4 EX Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("2572", "Minolta/Sony AF 500mm F8 Reflex");
        ExifServiceImpl.SONY_OBJECTIFS.put("2578", "Minolta/Sony AF 16mm F2.8 Fisheye or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2578.1", "Sigma 8mm F4 EX [DG] Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("2578.2", "Sigma 14mm F3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2578.3", "Sigma 15mm F2.8 Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("2579", "Minolta/Sony AF 20mm F2.8 or Tokina Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2579.1", "Tokina AT-X Pro DX 11-16mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("2581", "Minolta AF 100mm F2.8 Macro [New] or Sigma or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2581.1", "Sigma AF 90mm F2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2581.2", "Sigma AF 105mm F2.8 EX [DG] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2581.3", "Sigma 180mm F5.6 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2581.4", "Sigma 180mm F3.5 EX DG Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2581.5", "Tamron 90mm F2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2585", "Minolta AF 35-105mm F3.5-4.5 New or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2585.1", "Beroflex 35-135mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2585.2", "Tamron 24-135mm F3.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2588", "Minolta AF 70-210mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("2589", "Minolta AF 80-200mm F2.8 APO or Tokina Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("2589.1", "Tokina 80-200mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("2590", "Minolta AF 200mm F2.8 G APO + Minolta AF 1.4x APO or Other Lens + 1.4x");
        ExifServiceImpl.SONY_OBJECTIFS.put("2590.1", "Minolta AF 600mm F4 HS-APO G + Minolta AF 1.4x APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2591", "Minolta AF 35mm F1.4");
        ExifServiceImpl.SONY_OBJECTIFS.put("2592", "Minolta AF 85mm F1.4 G (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("2593", "Minolta AF 200mm F2.8 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2594", "Minolta AF 3x-1x F1.7-2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2596", "Minolta AF 28mm F2");
        ExifServiceImpl.SONY_OBJECTIFS.put("2597", "Minolta AF 35mm F2 [New]");
        ExifServiceImpl.SONY_OBJECTIFS.put("2598", "Minolta AF 100mm F2");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("2601", "Minolta AF 200mm F2.8 G APO + Minolta AF 2x APO or Other Lens + 2x");
        ExifServiceImpl.SONY_OBJECTIFS.put("2601.1", "Minolta AF 600mm F4 HS-APO G + Minolta AF 2x APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2604", "Minolta AF 80-200mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2605", "Minolta AF 35-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2606", "Minolta AF 100-300mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2607", "Minolta AF 35-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("2608", "Minolta AF 300mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("2609", "Minolta AF 600mm F4 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("2612", "Minolta AF 200mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("2613", "Minolta AF 50mm F1.7 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2615", "Minolta AF 28-105mm F3.5-4.5 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("2616", "Minolta AF 35-200mm F4.5-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("2618", "Minolta AF 28-80mm F4-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("2619", "Minolta AF 80-200mm F4.5-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("2620", "Minolta AF 28-70mm F2.8 G");
        ExifServiceImpl.SONY_OBJECTIFS.put("2621", "Minolta AF 100-300mm F4.5-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("2624", "Minolta AF 35-80mm F4-5.6 Power Zoom");
        ExifServiceImpl.SONY_OBJECTIFS.put("2628", "Minolta AF 80-200mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("2629", "Minolta AF 85mm F1.4 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2631", "Minolta AF 100-300mm F4.5-5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2632", "Minolta AF 24-50mm F4 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2638", "Minolta AF 50mm F2.8 Macro New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2639", "Minolta AF 100mm F2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("2641", "Minolta/Sony AF 20mm F2.8 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2642", "Minolta AF 24mm F2.8 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2644", "Minolta AF 100-400mm F4.5-6.7 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("2662", "Minolta AF 50mm F1.4 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2667", "Minolta AF 35mm F2 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2668", "Minolta AF 28mm F2 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("2672", "Minolta AF 24-105mm F3.5-4.5 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("3046", "Metabones Canon EF Speed Booster");
        ExifServiceImpl.SONY_OBJECTIFS.put("4567", "Tokina 70-210mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("4568", "Tokina AF 35-200mm F4-5.6 Zoom SD");
        ExifServiceImpl.SONY_OBJECTIFS.put("4570", "Tamron AF 35-135mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("4571", "Vivitar 70-210mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("4574", "2x Teleconverter or Tamron or Tokina Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("4574.1", "Tamron SP AF 90mm F2.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("4574.2", "Tokina RF 500mm F8.0 x2");
        ExifServiceImpl.SONY_OBJECTIFS.put("4574.3", "Tokina 300mm F2.8 x2");
        ExifServiceImpl.SONY_OBJECTIFS.put("4575", "1.4x Teleconverter");
        ExifServiceImpl.SONY_OBJECTIFS.put("4585", "Tamron SP AF 300mm F2.8 LD IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("4586", "Tamron SP AF 35-105mm F2.8 LD Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("4587", "Tamron AF 70-210mm F2.8 SP LD");
        ExifServiceImpl.SONY_OBJECTIFS.put("4812", "Metabones Canon EF Speed Booster Ultra");
        ExifServiceImpl.SONY_OBJECTIFS.put("6118", "Canon EF Adapter");
        ExifServiceImpl.SONY_OBJECTIFS.put("6528", "Sigma 16mm F2.8 Filtermatic Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553", "E-Mount, T-Mount, Other Lens or no lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.1", "Arax MC 35mm F2.8 Tilt+Shift");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.2", "Arax MC 80mm F2.8 Tilt+Shift");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.3", "Zenitar MF 16mm F2.8 Fisheye M42");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.4", "Samyang 500mm Mirror F8.0");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.5", "Pentacon Auto 135mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.6", "Pentacon Auto 29mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("6553.7", "Helios 44-2 58mm F2.0");
        ExifServiceImpl.SONY_OBJECTIFS.put("18688", "Sigma MC-11 SA-E Mount Converter with not-supported Sigma lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25501", "Minolta AF 50mm F1.7");
        ExifServiceImpl.SONY_OBJECTIFS.put("25511", "Minolta AF 35-70mm F4 or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25511.1", "Sigma UC AF 28-70mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25511.2", "Sigma AF 28-70mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("25511.3", "Sigma M-AF 70-200mm F2.8 EX Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("25511.4", "Quantaray M-AF 35-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("25511.5", "Tokina 28-70mm F2.8-4.5 AF");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521", "Minolta AF 28-85mm F3.5-4.5 or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.1", "Tokina 19-35mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.2", "Tokina 28-70mm F2.8 AT-X");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.3", "Tokina 80-400mm F4.5-5.6 AT-X AF II 840");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.4", "Tokina AF PRO 28-80mm F2.8 AT-X 280");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.5", "Tokina AT-X PRO [II] AF 28-70mm F2.6-2.8 270");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.6", "Tamron AF 19-35mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.7", "Angenieux AF 28-70mm F2.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.8", "Tokina AT-X 17 AF 17mm F3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25521.9", "Tokina 20-35mm F3.5-4.5 II AF");
        ExifServiceImpl.SONY_OBJECTIFS.put("25531", "Minolta AF 28-135mm F4-4.5 or Other Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25531.1", "Sigma ZOOM-alpha 35-135mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25531.2", "Sigma 28-105mm F2.8-4 Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("25531.3", "Sigma 28-105mm F4-5.6 UC");
        ExifServiceImpl.SONY_OBJECTIFS.put("25531.4", "Tokina AT-X 242 AF 24-200mm F3.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("25541", "Minolta AF 35-105mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25551", "Minolta AF 70-210mm F4 Macro or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25551.1", "Sigma 70-210mm F4-5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25551.2", "Sigma M-AF 70-200mm F2.8 EX APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25551.3", "Sigma 75-200mm F2.8-3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25561", "Minolta AF 135mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("25571", "Minolta/Sony AF 28mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("25581", "Minolta AF 24-50mm F4");
        ExifServiceImpl.SONY_OBJECTIFS.put("25601", "Minolta AF 100-200mm F4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611", "Minolta AF 75-300mm F4.5-5.6 or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.1", "Sigma 70-300mm F4-5.6 DL Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.2", "Sigma 300mm F4 APO Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.3", "Sigma AF 500mm F4.5 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.4", "Sigma AF 170-500mm F5-6.3 APO Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.5", "Tokina AT-X AF 300mm F4");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.6", "Tokina AT-X AF 400mm F5.6 SD");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.7", "Tokina AF 730 II 75-300mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.8", "Sigma 800mm F5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.9", "Sigma AF 400mm F5.6 APO Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25611.10", "Sigma 1000mm F8 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25621", "Minolta AF 50mm F1.4 [New]");
        ExifServiceImpl.SONY_OBJECTIFS.put("25631", "Minolta AF 300mm F2.8 APO or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25631.1", "Sigma AF 50-500mm F4-6.3 EX DG APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25631.2", "Sigma AF 170-500mm F5-6.3 APO Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("25631.3", "Sigma AF 500mm F4.5 EX DG APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25631.4", "Sigma 400mm F5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25641", "Minolta AF 50mm F2.8 Macro or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25641.1", "Sigma 50mm F2.8 EX Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25651", "Minolta AF 600mm F4 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25661", "Minolta AF 24mm F2.8 or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25661.1", "Sigma 17-35mm F2.8-4 EX Aspherical");
        ExifServiceImpl.SONY_OBJECTIFS.put("25721", "Minolta/Sony AF 500mm F8 Reflex");
        ExifServiceImpl.SONY_OBJECTIFS.put("25781", "Minolta/Sony AF 16mm F2.8 Fisheye or Sigma Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25781.1", "Sigma 8mm F4 EX [DG] Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("25781.2", "Sigma 14mm F3.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25781.3", "Sigma 15mm F2.8 Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("25791", "Minolta/Sony AF 20mm F2.8 or Tokina Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25791.1", "Tokina AT-X Pro DX 11-16mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("25811", "Minolta AF 100mm F2.8 Macro [New] or Sigma or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25811.1", "Sigma AF 90mm F2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25811.2", "Sigma AF 105mm F2.8 EX [DG] Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25811.3", "Sigma 180mm F5.6 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25811.4", "Sigma 180mm F3.5 EX DG Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25811.5", "Tamron 90mm F2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25851", "Beroflex 35-135mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25858", "Minolta AF 35-105mm F3.5-4.5 New or Tamron Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25858.1", "Tamron 24-135mm F3.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("25881", "Minolta AF 70-210mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("25891", "Minolta AF 80-200mm F2.8 APO or Tokina Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("25891.1", "Tokina 80-200mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("25901", "Minolta AF 200mm F2.8 G APO + Minolta AF 1.4x APO or Other Lens + 1.4x");
        ExifServiceImpl.SONY_OBJECTIFS.put("25901.1", "Minolta AF 600mm F4 HS-APO G + Minolta AF 1.4x APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25911", "Minolta AF 35mm F1.4");
        ExifServiceImpl.SONY_OBJECTIFS.put("25921", "Minolta AF 85mm F1.4 G (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("25931", "Minolta AF 200mm F2.8 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("25941", "Minolta AF 3x-1x F1.7-2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("25961", "Minolta AF 28mm F2");
        ExifServiceImpl.SONY_OBJECTIFS.put("25971", "Minolta AF 35mm F2 [New]");
        ExifServiceImpl.SONY_OBJECTIFS.put("25981", "Minolta AF 100mm F2");
        ExifServiceImpl.SONY_OBJECTIFS
            .put("26011", "Minolta AF 200mm F2.8 G APO + Minolta AF 2x APO or Other Lens + 2x");
        ExifServiceImpl.SONY_OBJECTIFS.put("26011.1", "Minolta AF 600mm F4 HS-APO G + Minolta AF 2x APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("26041", "Minolta AF 80-200mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("26051", "Minolta AF 35-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("26061", "Minolta AF 100-300mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("26071", "Minolta AF 35-80mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("26081", "Minolta AF 300mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("26091", "Minolta AF 600mm F4 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("26121", "Minolta AF 200mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("26131", "Minolta AF 50mm F1.7 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26151", "Minolta AF 28-105mm F3.5-4.5 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("26161", "Minolta AF 35-200mm F4.5-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("26181", "Minolta AF 28-80mm F4-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("26191", "Minolta AF 80-200mm F4.5-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("26201", "Minolta AF 28-70mm F2.8 G");
        ExifServiceImpl.SONY_OBJECTIFS.put("26211", "Minolta AF 100-300mm F4.5-5.6 xi");
        ExifServiceImpl.SONY_OBJECTIFS.put("26241", "Minolta AF 35-80mm F4-5.6 Power Zoom");
        ExifServiceImpl.SONY_OBJECTIFS.put("26281", "Minolta AF 80-200mm F2.8 HS-APO G");
        ExifServiceImpl.SONY_OBJECTIFS.put("26291", "Minolta AF 85mm F1.4 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26311", "Minolta AF 100-300mm F4.5-5.6 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("26321", "Minolta AF 24-50mm F4 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26381", "Minolta AF 50mm F2.8 Macro New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26391", "Minolta AF 100mm F2.8 Macro");
        ExifServiceImpl.SONY_OBJECTIFS.put("26411", "Minolta/Sony AF 20mm F2.8 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26421", "Minolta AF 24mm F2.8 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26441", "Minolta AF 100-400mm F4.5-6.7 APO");
        ExifServiceImpl.SONY_OBJECTIFS.put("26621", "Minolta AF 50mm F1.4 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26671", "Minolta AF 35mm F2 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26681", "Minolta AF 28mm F2 New");
        ExifServiceImpl.SONY_OBJECTIFS.put("26721", "Minolta AF 24-105mm F3.5-4.5 (D)");
        ExifServiceImpl.SONY_OBJECTIFS.put("30464", "Metabones Canon EF Speed Booster");
        ExifServiceImpl.SONY_OBJECTIFS.put("45671", "Tokina 70-210mm F4-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("45681", "Tokina AF 35-200mm F4-5.6 Zoom SD");
        ExifServiceImpl.SONY_OBJECTIFS.put("45701", "Tamron AF 35-135mm F3.5-4.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("45711", "Vivitar 70-210mm F4.5-5.6");
        ExifServiceImpl.SONY_OBJECTIFS.put("45741", "2x Teleconverter or Tamron or Tokina Lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("45741.1", "Tamron SP AF 90mm F2.5");
        ExifServiceImpl.SONY_OBJECTIFS.put("45741.2", "Tokina RF 500mm F8.0 x2");
        ExifServiceImpl.SONY_OBJECTIFS.put("45741.3", "Tokina 300mm F2.8 x2");
        ExifServiceImpl.SONY_OBJECTIFS.put("45751", "1.4x Teleconverter");
        ExifServiceImpl.SONY_OBJECTIFS.put("45851", "Tamron SP AF 300mm F2.8 LD IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("45861", "Tamron SP AF 35-105mm F2.8 LD Aspherical IF");
        ExifServiceImpl.SONY_OBJECTIFS.put("45871", "Tamron AF 70-210mm F2.8 SP LD");
        ExifServiceImpl.SONY_OBJECTIFS.put("48128", "Metabones Canon EF Speed Booster Ultra");
        ExifServiceImpl.SONY_OBJECTIFS.put("61184", "Canon EF Adapter");
        ExifServiceImpl.SONY_OBJECTIFS.put("65280", "Sigma 16mm F2.8 Filtermatic Fisheye");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535", "E-Mount, T-Mount, Other Lens or no lens");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.1", "Arax MC 35mm F2.8 Tilt+Shift");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.2", "Arax MC 80mm F2.8 Tilt+Shift");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.3", "Zenitar MF 16mm F2.8 Fisheye M42");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.4", "Samyang 500mm Mirror F8.0");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.5", "Pentacon Auto 135mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.6", "Pentacon Auto 29mm F2.8");
        ExifServiceImpl.SONY_OBJECTIFS.put("65535.7", "Helios 44-2 58mm F2.0");
    }

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

    @Override
    public byte[] getSonyLens(int[] lens) {
        try {
            final String lensId = Integer.toString(lens[0]);
            if (ExifServiceImpl.SONY_OBJECTIFS.containsKey(lensId)) {
                return ExifServiceImpl.SONY_OBJECTIFS.get(lensId)
                    .getBytes("UTF-8");
            }
            return null;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
        ExifFileRecord efr = infoDesc.orElseThrow(
            () -> {
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
        if (data != null) {
            switch (fieldType) {
                case ASCII: {
                    if (data instanceof byte[]) {
                        byte[] valueAsByte = (byte[]) data;
                        return new String(valueAsByte, 0, valueAsByte.length - 1, Charset.forName("UTF-8"));
                    } else if (data instanceof String) { return (String) data; }
                    return "<not processed>";
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
        }
        return "<data is null>";
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
            ExifServiceImpl.LOGGER.debug(
                " unable to find  tag " + Integer.toHexString(tag.getValue()) + " not found in "
                    + Integer.toHexString(shortValueOfifdParent));
            return new IllegalArgumentException(" tag " + Integer.toHexString(tag.getValue()) + " not found in "
                + Integer.toHexString(shortValueOfifdParent));
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
