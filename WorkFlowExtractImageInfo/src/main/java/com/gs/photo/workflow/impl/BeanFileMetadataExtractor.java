package com.gs.photo.workflow.impl;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photos.workflow.metadata.AbstractTemplateTag;
import com.gs.photos.workflow.metadata.FileChannelDataInput;
import com.gs.photos.workflow.metadata.IFD;
import com.gs.photos.workflow.metadata.IOUtils;
import com.gs.photos.workflow.metadata.ReadStrategyII;
import com.gs.photos.workflow.metadata.ReadStrategyMM;
import com.gs.photos.workflow.metadata.TemplateTagFactory;

@Service
public class BeanFileMetadataExtractor implements IFileMetadataExtractor {

	public static final int STREAM_HEAD = 0x00;

	private static Logger LOGGER = LoggerFactory.getLogger(IFileMetadataExtractor.class);

	@Override
	public Collection<IFD> readIFDs(Path filePath) {
		Collection<IFD> allIfds = new ArrayList<>();
		try (FileChannelDataInput fcdi = new FileChannelDataInput()) {
			FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
			fcdi.setFileChannel(fileChannel);
			int offset = readHeader(fcdi);
			do {
				AbstractTemplateTag dtp = TemplateTagFactory.create();
				offset = dtp.createSimpleTiffFields(fcdi, offset);
				allIfds.addAll(dtp.getAllIfds());
			} while (offset != 0);
			Collection<IFD> allIFD = getAllIFDs(allIfds);
			return allIFD;
		} catch (IOException e) {
			LOGGER.error("Error...", e);
			throw new RuntimeException(e);
		}
	}

	private Collection<IFD> getAllIFDs(Collection<IFD> allIfds) {
		final Collection<IFD> retValue = new ArrayList<>();
		allIfds.forEach((ifd) -> {
			retValue.add(ifd);
			retValue.addAll(getAllIFDs(ifd.getAllChildren()));
		});
		return retValue;
	}

	private int readHeader(FileChannelDataInput rin) throws IOException {
		int offset = 0;
		rin.position(STREAM_HEAD);
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
