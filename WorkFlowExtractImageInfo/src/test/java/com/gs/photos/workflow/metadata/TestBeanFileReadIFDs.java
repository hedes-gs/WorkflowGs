package com.gs.photos.workflow.metadata;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photos.workflow.metadata.tiff.TiffField;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TestBeanFileReadIFDs {
	private Logger LOGGER = LogManager.getLogger(TestBeanFileReadIFDs.class);

	@Autowired
	protected IFileMetadataExtractor beanFileMetadataExtractor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void shouldGet204TiffFieldWhenSonyARWIsAnInputFile() {
		Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();

		Collection<IFD> allIfds = beanFileMetadataExtractor.readIFDs(filePath);
		Collection<TiffField<?>> allTiff = getAllTiffFields(allIfds);
		assertEquals(204, allTiff.size());
	}

	@Test
	public void shouldGet2JpgFilesWhenSonyARWIsAnInputFile() {
		Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();

		Collection<IFD> allIfds = beanFileMetadataExtractor.readIFDs(filePath);
		Collection<TiffField<?>> allTiff = getAllTiffFields(allIfds);
		allTiff.forEach((tif) -> LOGGER.info(tif));

		assertEquals(2, allIfds.stream().filter((ifd) -> ifd.imageIsPresent()).count());

		allIfds.stream().filter((ifd) -> ifd.imageIsPresent()).map((ifd) -> ifd.getJpegImage()).forEach((img) -> {
			LocalDateTime currentTime = LocalDateTime.now();
			try (FileOutputStream stream = new FileOutputStream(
					UUID.randomUUID() + "-" + currentTime.toString().replaceAll("\\:", "_") + ".jpg")) {
				stream.write(img);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
	}

	private Collection<TiffField<?>> getAllTiffFields(Collection<IFD> allIfds) {
		final Collection<TiffField<?>> retValue = new ArrayList<>();
		allIfds.forEach((ifd) -> {
			retValue.addAll(ifd.getFields());
			retValue.addAll(getAllTiffFields(ifd.getAllChildren()));
		});
		return retValue;
	}

}
