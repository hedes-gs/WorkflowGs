package com.images.images;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Ignore;
import org.junit.Test;

import com.gs.photos.serializers.ExchangedDataSerializer;
import com.icafe4j.image.meta.Metadata;
import com.icafe4j.image.tiff.TIFFTweaker;
import com.icafe4j.image.tiff.TiffTag;
import com.icafe4j.io.FileChannelDataInput;
import com.workflow.model.ExchangedTiffData;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.reactivex.internal.schedulers.ExecutorScheduler;

public class TestFileChannelDataInput {

	@Test
	@Ignore
	public void testReadHeader() {
		Path path = FileSystems.getDefault().getPath(
			"C:\\Users\\deslandh\\Downloads",
			"sony-a9-9e92817a - Copy.tiff");
		;
		try (FileChannel fc = FileChannel.open(
			path,
			StandardOpenOption.READ)) {
			FileChannelDataInput fcdi = new FileChannelDataInput();
			fcdi.setFileChannel(
				fc);
			byte[] bc = new byte[3 * 1024 * 1024];
			fcdi.readFully(
				bc);
			String hex = null; // DatatypeConverter.printHexBinary(bc);
			System.out.println(
				hex);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	@Ignore

	public void testExtractThumbnail() {
		Path path = FileSystems.getDefault().getPath(
			"H:",
			"sony-a9-9e92817a - Copy.tiff");
		;
		try (FileChannel fc = FileChannel.open(
			path,
			StandardOpenOption.READ)) {
			FileChannelDataInput fcdi = new FileChannelDataInput();
			fcdi.setFileChannel(
				fc);
			Metadata.extractThumbnails(
				fcdi,
				"thumbnail");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static <V extends ExchangedTiffData> byte[] toBytesGeneric(final V t,
			final Class<? extends ExchangedTiffData> class1) {
		final ByteArrayOutputStream bout = new ByteArrayOutputStream();
		try {
			final Schema schema = ReflectData.get().getSchema(
				class1);
			final DatumWriter<V> writer = new ReflectDatumWriter<V>(schema);
			final BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(
				bout,
				null);
			try {
				writer.write(
					t,
					binEncoder);
				binEncoder.flush();
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return bout.toByteArray();
	}

	@Test
	@Ignore

	public void testExtractAllUsefulData() {
		Path path = FileSystems.getDefault().getPath(
			"D:\\photos\\danses\\champ-latine-2018\\Capture\\2018-05-06 09.21.29",
			"_HDE1001.ARW");

		Properties settings = new Properties();
		// Set a few key parameters
		settings.put(
			StreamsConfig.APPLICATION_ID_CONFIG,
			"my-first-streams-application");
		settings.put(
			StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
			"localhost:9092");
		settings.put(
			"key.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");
		settings.put(
			"value.serializer",
			"org.apache.kafka.common.serialization.StringSerializer");

		final String key = Long.toHexString(
			System.nanoTime()) + "-" + path.getFileName();

		try (Producer<String, String> producer = new KafkaProducer<>(settings)) {
			producer.send(
				new ProducerRecord<>("image", key, path.toAbsolutePath().toString()));
			System.out.println(
				"......... image sent");
		}

		settings.put(
			"value.serializer",
			ExchangedDataSerializer.class.getName());
		try (Producer<String, ExchangedTiffData> producer = new KafkaProducer<>(settings)) {
			try (FileChannel fc = FileChannel.open(
				path,
				StandardOpenOption.READ)) {
				FileChannelDataInput fcdi = new FileChannelDataInput();
				fcdi.setFileChannel(
					fc);
				Flowable<ExchangedTiffData> flow = TIFFTweaker.createStream(
					fcdi,
					key);
				flow.observeOn(
					new ExecutorScheduler(
						Executors.newFixedThreadPool(
							2)))
						.subscribe(
							new Consumer<ExchangedTiffData>() {

								@Override
								public void accept(ExchangedTiffData t) throws Exception {
									if (t.getTag() == TiffTag.THUMB_JPEG.getValue()) {
										System.out.println(
											"......... thumb sent");
										producer.send(
											new ProducerRecord("thumb", t.getId(), t));
									} else {
										producer.send(
											new ProducerRecord("exif", t.getId(), t));
									}
								}
							});
				synchronized (this) {
					wait();
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
