package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.exif.IExifService;
import com.gs.photos.workflow.metadata.AbstractTemplateTag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.IFD;
import com.gs.photos.workflow.metadata.IOUtils;
import com.gs.photos.workflow.metadata.ReadStrategyII;
import com.gs.photos.workflow.metadata.ReadStrategyMM;
import com.gs.photos.workflow.metadata.TemplateTagFactory;
import com.gs.photos.workflow.metadata.exif.RootTiffTag;

@Service
public class BeanFileMetadataExtractor implements IFileMetadataExtractor {

    public static final int STREAM_HEAD = 0x00;

    private static Logger   LOGGER      = LoggerFactory.getLogger(IFileMetadataExtractor.class);

    @Autowired
    protected IIgniteDAO    iIgniteDAO;

    @Autowired
    protected IExifService  exifService;

    @Override
    public Collection<IFD> readIFDs(String key) {
        return this.iIgniteDAO.get(key)
            .map((buffer) -> this.processFile(buffer, key))
            .orElseThrow(() -> new IllegalArgumentException("Key " + key + " is not present in cache"));
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
            throw new RuntimeException(e);
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
            throw new RuntimeException("Invalid TIFF byte order");
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
