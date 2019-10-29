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
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.IFileMetadataExtractor;
import com.gs.photo.workflow.IIgniteDAO;
import com.gs.photo.workflow.impl.BeanFileMetadataExtractor;
import com.gs.photos.workflow.metadata.tiff.TiffField;

@ActiveProfiles("test")
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { NameServiceTestConfiguration.class, BeanFileMetadataExtractor.class })
public class TestBeanFileReadIFDs {
	private Logger                   LOGGER = LogManager.getLogger(TestBeanFileReadIFDs.class);

	@Autowired
	IIgniteDAO                       iIgniteDAO;

	@Autowired
	protected IFileMetadataExtractor beanFileMetadataExtractor;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
		Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();
		FileChannel fc = FileChannel.open(filePath,
				StandardOpenOption.READ);
		ByteBuffer bb = ByteBuffer.allocate(4 * 1024 * 1024);
		fc.read(bb);
		Mockito.when(this.iIgniteDAO.get("1")).thenReturn(bb.array());
	}

	@Test
	public void shouldGet204TiffFieldWhenSonyARWIsAnInputFile() {

		Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1");
		Collection<TiffField<?>> allTiff = this.getAllTiffFields(allIfds);
		Assert.assertEquals(204,
				allTiff.size());
	}

	@Test
	public void shouldGet2JpgFilesWhenSonyARWIsAnInputFile() {

		Collection<IFD> allIfds = this.beanFileMetadataExtractor.readIFDs("1");
		Collection<TiffField<?>> allTiff = this.getAllTiffFields(allIfds);
		allTiff.forEach((tif) -> this.LOGGER.info(tif));

		Assert.assertEquals(2,
				allIfds.stream().filter((ifd) -> ifd.imageIsPresent()).count());

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
	}

	private Collection<TiffField<?>> getAllTiffFields(Collection<IFD> allIfds) {
		final Collection<TiffField<?>> retValue = new ArrayList<>();
		allIfds.forEach((ifd) -> {
			retValue.addAll(ifd.getFields());
			retValue.addAll(this.getAllTiffFields(ifd.getAllChildren()));
		});
		return retValue;
	}

}
