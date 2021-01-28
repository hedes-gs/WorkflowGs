package com.gs.photo.workflow.extimginfo.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.IIgniteDAO;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photos.workflow.extimginfo.metadata.AbstractTemplateTag;
import com.gs.photos.workflow.extimginfo.metadata.FileChannelDataInput;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.IOUtils;
import com.gs.photos.workflow.extimginfo.metadata.ReadStrategyII;
import com.gs.photos.workflow.extimginfo.metadata.ReadStrategyMM;
import com.gs.photos.workflow.extimginfo.metadata.TemplateTagFactory;
import com.gs.photos.workflow.extimginfo.metadata.exif.RootTiffTag;

@Service
public class BeanFileMetadataExtractor implements IFileMetadataExtractor {

    public static final int STREAM_HEAD = 0x00;

    private static Logger   LOGGER      = LoggerFactory.getLogger(IFileMetadataExtractor.class);

    @Autowired
    protected IIgniteDAO    iIgniteDAO;

    @Autowired
    protected IExifService  exifService;

    @Override
    public Optional<Collection<IFD>> readIFDs(String key) {
        Optional<byte[]> igniteValue = this.iIgniteDAO.get(key);
        igniteValue.ifPresentOrElse(
            (t) -> {},
            () -> BeanFileMetadataExtractor.LOGGER.warn("Unable to get key " + key + " in Ignite"));
        Optional<Collection<IFD>> values = igniteValue.map((b) -> this.processFile(b, key));
        return values;
    }

    private Collection<IFD> processFile(byte[] buffer, String key) {
        IFD.IFDContext ifdContext = new IFD.IFDContext();
        Collection<IFD> allIfds = new ArrayList<>();
        try (
            FileChannelDataInput fcdi = new FileChannelDataInput(buffer)) {

            int offset = this.readHeader(fcdi);
            for (RootTiffTag rtt : RootTiffTag.values()) {
                AbstractTemplateTag dtp = TemplateTagFactory.create(rtt, this.exifService);
                offset = dtp.createSimpleTiffFields(fcdi, offset, ifdContext);
                allIfds.add(dtp.getRootIFD());
                if (offset == 0) {
                    break;
                }
            }
            return allIfds;
        } catch (IOException e) {
            BeanFileMetadataExtractor.LOGGER
                .error("Error when processing {} : {} ", key, ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    private int readHeader(FileChannelDataInput rin) throws IOException {
        int offset = 0;
        rin.position(BeanFileMetadataExtractor.STREAM_HEAD);
        short endian = rin.readShort();
        offset += 2;
        if (endian == IOUtils.BIG_ENDIAN) {
            rin.setReadStrategy(ReadStrategyMM.getInstance());
        } else if (endian == IOUtils.LITTLE_ENDIAN) {
            rin.setReadStrategy(ReadStrategyII.getInstance());
        } else {
            throw new IOException("Invalid TIFF byte order : " + Integer.toHexString(endian) + " expected value "
                + Integer.toHexString(IOUtils.BIG_ENDIAN) + " or " + Integer.toHexString(IOUtils.LITTLE_ENDIAN));
        }
        // Read TIFF identifier
        rin.position(offset);
        short tiff_id = rin.readShort();
        offset += 2;

        if (tiff_id != 0x2a) { // "*" 42 decimal
            throw new RuntimeException("Invalid TIFF identifier");
        }

        rin.position(offset);
        offset = rin.readInt();

        return offset;
    }

}
