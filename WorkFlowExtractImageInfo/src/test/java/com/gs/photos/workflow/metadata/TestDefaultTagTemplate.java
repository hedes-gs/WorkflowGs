package com.gs.photos.workflow.metadata;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
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

import com.gs.photos.workflow.metadata.exif.RootTiffTag;
import com.gs.photos.workflow.metadata.tiff.TiffField;

public class TestDefaultTagTemplate {
	public static final int        STREAM_HEAD = 0x00;
	private Logger                 LOGGER      = LogManager.getLogger(TestDefaultTagTemplate.class);

	protected FileChannelDataInput fcdi;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
		FileChannel fc = FileChannel.open(filePath,
				StandardOpenOption.READ);
		ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
		fc.read(bb);
		this.fcdi = new FileChannelDataInput(bb.array());
	}

	@Test
	public void testConvertTagValueToTag() {
	}

	@Test
	public void testCreateSimpleTiffFields() {
		List<IFD> allIfds = new ArrayList<>();
		try {
			int offset = this.readHeader(this.fcdi);
			do {
				AbstractTemplateTag dtp = TemplateTagFactory.create(RootTiffTag.ROOT_TIFF);
				offset = dtp.createSimpleTiffFields(this.fcdi,
						offset);
				allIfds.addAll(dtp.getAllIfds());
			} while (offset != 0);
			Collection<TiffField<?>> allTiff = this.getAllTiffFields(allIfds);

			allIfds.forEach((ifd) -> this.LOGGER.info(ifd));
			allTiff.forEach((tif) -> this.LOGGER.info(tif));

			allIfds.stream().filter((ifd) -> ifd.imageIsPresent()).map((ifd) -> ifd.getJpegImage()).forEach((img) -> {
				LocalDateTime currentTime = LocalDateTime.now();
				try (
						FileOutputStream stream = new FileOutputStream(
							UUID.randomUUID() + "-" + currentTime.toString().replaceAll("\\:",
									"_") + ".jpg")) {
					stream.write(img);
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
		allIfds.forEach((ifd) -> {
			retValue.addAll(ifd.getFields());
			retValue.addAll(this.getAllTiffFields(ifd.getAllChildren()));
		});
		return retValue;
	}

	private int readHeader(FileChannelDataInput rin) throws IOException {
		int offset = 0;
		rin.position(TestDefaultTagTemplate.STREAM_HEAD);
		short endian = rin.readShort();
		offset += 2;

		if (endian == IOUtils.BIG_ENDIAN) {
			rin.setReadStrategy(ReadStrategyMM.getInstance());
		} else if (endian == IOUtils.LITTLE_ENDIAN) {
			rin.setReadStrategy(ReadStrategyII.getInstance());
		} else {
			throw new RuntimeException("Invalid TIFF byte order");
		}
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
