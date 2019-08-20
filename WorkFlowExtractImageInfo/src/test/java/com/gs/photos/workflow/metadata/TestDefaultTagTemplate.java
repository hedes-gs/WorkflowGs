package com.gs.photos.workflow.metadata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.gs.photos.workflow.metadata.tiff.TiffField;

public class TestDefaultTagTemplate {
	public static final int STREAM_HEAD = 0x00;
	private Logger LOGGER = LogManager.getLogger(
		TestDefaultTagTemplate.class);

	protected FileChannelDataInput fcdi;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
		fcdi = new FileChannelDataInput();
		FileChannel fileChannel = FileChannel.open(
			filePath,
			StandardOpenOption.READ);
		fcdi.setFileChannel(
			fileChannel);
	}

	@Test
	public void testConvertTagValueToTag() {
	}

	@Test
	public void testCreateSimpleTiffFields() {
		List<IFD> allIfds = new ArrayList<>();
		try {
			int offset = readHeader(
				fcdi);
			do {
				AbstractTemplateTag dtp = TemplateTagFactory.create();
				offset = dtp.createSimpleTiffFields(
					fcdi,
					offset);
				allIfds.addAll(
					dtp.getAllIfds());
			} while (offset != 0);
			Collection<TiffField<?>> allTiff = getAllTiffFields(
				allIfds);

			allIfds.forEach(
				(ifd) -> LOGGER.info(
					ifd));
			allTiff.forEach(
				(tif) -> LOGGER.info(
					tif));

			allIfds.stream().filter(
				(ifd) -> ifd.imageIsPresent()).map(
					(ifd) -> ifd.getJpegImage()).forEach(
						(img) -> {
							LocalDateTime currentTime = LocalDateTime.now();
							try (FileOutputStream stream = new FileOutputStream(
								UUID.randomUUID() + "-" + currentTime.toString().replaceAll(
									"\\:",
									"_") + ".jpg")) {
								stream.write(
									img);
							} catch (FileNotFoundException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private Collection<TiffField<?>> getAllTiffFields(Collection<IFD> allIfds) {
		final Collection<TiffField<?>> retValue = new ArrayList<>();
		allIfds.forEach(
			(ifd) -> {
				retValue.addAll(
					ifd.getFields());
				retValue.addAll(
					getAllTiffFields(
						ifd.getAllChildren()));
			});
		return retValue;
	}

	private int readHeader(FileChannelDataInput rin) throws IOException {
		int offset = 0;
		rin.position(
			STREAM_HEAD);
		short endian = rin.readShort();
		offset += 2;

		if (endian == IOUtils.BIG_ENDIAN) {
			rin.setReadStrategy(
				ReadStrategyMM.getInstance());
		} else if (endian == IOUtils.LITTLE_ENDIAN) {
			rin.setReadStrategy(
				ReadStrategyII.getInstance());
		} else {
			throw new RuntimeException("Invalid TIFF byte order");
		}
		rin.position(
			offset);
		short tiff_id = rin.readShort();
		offset += 2;

		if (tiff_id != 0x2a) { // "*" 42 decimal
			throw new RuntimeException("Invalid TIFF identifier");
		}

		rin.position(
			offset);
		offset = rin.readInt();

		return offset;
	}

}
