package com.gs.photo.workflow.extimginfo.impl;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.workflow.extimginfo.IFileMetadataExtractor;
import com.gs.photo.workflow.extimginfo.ports.IAccessDirectlyFile;
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

    public static final int       STREAM_HEAD = 0x00;

    private static Logger         LOGGER      = LoggerFactory.getLogger(BeanFileMetadataExtractor.class);

    @Autowired
    protected IExifService        exifService;

    @Autowired
    protected IAccessDirectlyFile accessDirectlyFile;

    @Override
    public Optional<Collection<IFD>> readIFDs(Optional<byte[]> image, FileToProcess fileToProcess) {
        Optional<Collection<IFD>> values = image.map((b) -> {
            Collection<IFD> retValue = Collections.EMPTY_SET;
            boolean end = false;
            byte[] localbuffer = b;
            int nbOfTries = 1;
            boolean bufferAlreadyIncreased = false;
            do {
                try {
                    BeanFileMetadataExtractor.LOGGER.info(
                        "[EVENT][{}]Retries are {} -  Buffer size is {} ",
                        fileToProcess.getImageId(),
                        fileToProcess,
                        nbOfTries,
                        localbuffer.length);
                    retValue = this.processFile(localbuffer, fileToProcess.getImageId());
                    end = true;
                } catch (BufferUnderflowException e) {
                    if (bufferAlreadyIncreased) {
                        BeanFileMetadataExtractor.LOGGER.error(
                            "[EVENT][{}]Unable to process even after increasing it - ignoring ",
                            fileToProcess.getImageId());

                        end = true;
                    } else {
                        BeanFileMetadataExtractor.LOGGER
                            .warn("[EVENT][{}]Buffer too small, increasing it", fileToProcess.getImageId());
                        localbuffer = this.accessDirectlyFile
                            .readFirstBytesOfFileRetryWithbufferIncreased(fileToProcess)
                            .get();
                        bufferAlreadyIncreased = true;
                    }
                } catch (Exception e) {
                    BeanFileMetadataExtractor.LOGGER.error(
                        "[EVENT][{}]Unexpected Error when processing file {}, retries are {}, exception is {} ",
                        fileToProcess.getImageId(),
                        fileToProcess,
                        nbOfTries,
                        e.getMessage());

                    nbOfTries++;
                    if (nbOfTries > 2) {
                        BeanFileMetadataExtractor.LOGGER.error(
                            "[EVENT][{}]Unexpected Error when processing file {}: {} - nb of tries are {} ",
                            fileToProcess.getImageId(),
                            fileToProcess,
                            ExceptionUtils.getStackTrace(e),
                            nbOfTries);
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

            try {
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
            } catch (InvalidHeaderException e) {
                BeanFileMetadataExtractor.LOGGER.error(
                    "[EVENT][{}]Unexpected header when processing file - {} - return empty in order to skip it",
                    key,
                    e.getMessage(),
                    ExceptionUtils.getStackTrace(e));
                return Collections.EMPTY_LIST;
            }
        } catch (BufferUnderflowException e) {
            BeanFileMetadataExtractor.LOGGER.error(
                "[EVENT][{}]Buffer too small... when processing file: {} ",
                key,
                ExceptionUtils.getStackTrace(e));
            throw e;
        } catch (Exception e) {
            BeanFileMetadataExtractor.LOGGER
                .error("[EVENT][{}]Unexpected error when processing file: {} ", key, ExceptionUtils.getStackTrace(e));
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
            throw new InvalidHeaderException(
                "Invalid TIFF byte order : " + Integer.toHexString(endian) + " expected value "
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
