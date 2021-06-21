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
import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photos.workflow.extimginfo.metadata.AbstractTemplateTag;
import com.gs.photos.workflow.extimginfo.metadata.FileChannelDataInput;
import com.gs.photos.workflow.extimginfo.metadata.IFD;
import com.gs.photos.workflow.extimginfo.metadata.IOUtils;
import com.gs.photos.workflow.extimginfo.metadata.ReadStrategyII;
import com.gs.photos.workflow.extimginfo.metadata.ReadStrategyMM;
import com.gs.photos.workflow.extimginfo.metadata.TemplateTagFactory;
import com.gs.photos.workflow.extimginfo.metadata.exif.RootTiffTag;
import com.workflow.model.files.FileToProcess;

@Service
public class BeanFileMetadataExtractor implements IFileMetadataExtractor {

    public static final int STREAM_HEAD = 0x00;

    private static Logger   LOGGER      = LoggerFactory.getLogger(IFileMetadataExtractor.class);

    @Autowired
    protected IIgniteDAO    iIgniteDAO;

    @Autowired
    protected IExifService  exifService;

    @Autowired
    protected FileUtils     fileUtils;

    @Override
    public Optional<Collection<IFD>> readIFDs(FileToProcess fileToProcess) {
        Optional<Collection<IFD>> values = this.iIgniteDAO.get(fileToProcess.getImageId())
            .or(() -> {
                BeanFileMetadataExtractor.LOGGER.warn(
                    "[EVENT][{}]Unable to get key Ignite - get direct value from source {}",
                    fileToProcess.getImageId(),
                    fileToProcess);
                try {
                    return Optional.ofNullable(this.fileUtils.readFirstBytesOfFileRetry(fileToProcess));
                } catch (Exception e) {
                    BeanFileMetadataExtractor.LOGGER.warn(
                        "[EVENT][{}]Unable to get key Ignite for {},  unexpected error {}",
                        fileToProcess.getImageId(),
                        fileToProcess,
                        ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            })
            .map((b) -> {
                Collection<IFD> retValue = null;
                boolean end = false;
                byte[] localbuffer = b;
                int nbOfTries = 1;
                do {
                    try {
                        retValue = this.processFile(localbuffer, fileToProcess.getImageId());
                        end = true;
                    } catch (Exception e) {
                        BeanFileMetadataExtractor.LOGGER.error(
                            "[EVENT][{}]Unexpected Error when processing file {}, retries are {}, exception is {} ",
                            fileToProcess.getImageId(),
                            fileToProcess,
                            nbOfTries,
                            e.getMessage());
                        localbuffer = this.fileUtils.readFirstBytesOfFileRetryWithbufferIncreased(fileToProcess);
                        nbOfTries++;
                        if (nbOfTries > 2) {
                            BeanFileMetadataExtractor.LOGGER.error(
                                "[EVENT][{}]Unexpected Error when processing file {}: {} ",
                                fileToProcess.getImageId(),
                                fileToProcess,
                                ExceptionUtils.getStackTrace(e));
                            throw new RuntimeException(e);
                        }
                    }
                } while (!end);
                return retValue;
            });
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
        } catch (Exception e) {
            BeanFileMetadataExtractor.LOGGER
                .error("[EVENT][{}]Unexpected Error when processing file: {} ", key, ExceptionUtils.getStackTrace(e));
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
