package com.gs.photo.workflow.poll;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.gs.photo.workflow.IBeanImageFileHelper;
import com.icafe4j.image.tiff.TIFFTweaker;
import com.icafe4j.image.tiff.TiffTag;
import com.icafe4j.io.FileChannelDataInput;
import com.workflow.model.ExchangedTiffData;

import io.reactivex.Flowable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.schedulers.ExecutorScheduler;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BeanImageFileHelperTest {

	private static final ExecutorService NEW_FIXED_THREAD_POOL = Executors.newFixedThreadPool(2);

	@Autowired
	IBeanImageFileHelper beanImageFileHelper;

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void shouldComputeHashKeyWhenFileIsProvided() throws URISyntaxException, IOException {
		URL resource = getClass().getClassLoader().getResource("TestFile.ARW");
		File f = new File(resource.toURI());
		String key = beanImageFileHelper.computeHashKey(f.toPath());
		assertEquals(
				"e3a1177d5758a317083b10e8a4e329d451a8f16da966284425db50f51f314264323d16adc4781310087ceba1bd041ac7c0ba3fd271cb849881e2df6822225eac",
				key);

	}

	@Test
	public void shouldGetFullPathNameOfFile() throws URISyntaxException, IOException {
		URL resource = getClass().getClassLoader().getResource("TestFile.ARW");
		File f = new File(resource.toURI());
		String allAdresses = beanImageFileHelper.getFullPathName(f.toPath());
		System.err.println("...............  " + allAdresses);
	}

	@Test
	public void test() {
		try {
			Path filePath = new File("src/test/resources/_HDE0394.ARW").toPath();

			FileChannelDataInput fcdi = new FileChannelDataInput();
			FileChannel fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
			fcdi.setFileChannel(fileChannel);
			Flowable<ExchangedTiffData> flow = TIFFTweaker.createStream(fcdi, "one-key");

			fileChannel.close();
			fcdi.close();

			if (flow != null) {
				Disposable disposable = flow.observeOn(new ExecutorScheduler(NEW_FIXED_THREAD_POOL))
						.subscribe(new Consumer<ExchangedTiffData>() {

							@Override
							public void accept(ExchangedTiffData t) throws Exception {
								if (t.getTag() == TiffTag.THUMB_JPEG.getValue()) {
									System.out.println("getting the thumb for ");
								} else {
									System.out.println("... found " + Integer.toHexString(t.getTag()));

								}
							}
						});
				flow.doOnComplete(() -> {
					disposable.dispose();
				});

			}
		} catch (IOException e) {
			// TODO Auto-enerated catch block
			e.printStackTrace();
		}
	}

}
