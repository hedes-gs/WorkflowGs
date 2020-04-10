package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IIgniteDAO;
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

    @Override
    public Collection<IFD> readIFDs(String key) {
        Collection<IFD> allIfds = new ArrayList<>();
        try (
            FileChannelDataInput fcdi = new FileChannelDataInput(this.iIgniteDAO.get(key))) {

            int offset = this.readHeader(fcdi);
            for (RootTiffTag rtt : RootTiffTag.values()) {
                AbstractTemplateTag dtp = TemplateTagFactory.create(rtt);
                offset = dtp.createSimpleTiffFields(fcdi, offset);
                allIfds.add(dtp.getRootIFD());
                if (offset == 0) {
                    break;
                }
            }
            return allIfds;
        } catch (IOException e) {
            BeanFileMetadataExtractor.LOGGER.error("Error...", e);
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
