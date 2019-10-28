package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.IScan;

@Component
public class BeanScan implements IScan {

	protected static final org.slf4j.Logger LOGGER     = LoggerFactory.getLogger(IScan.class);

	@Value("${topic.scan-output}")
	protected String                        outputTopic;

	@Value("${topic.scan-output-child-parent}")
	protected String                        outputParentTopic;

	@Value("${scan.folder}")
	protected String                        folder;

	@Autowired
	protected Producer<String, String>      producerForPublishingOnStringTopic;

	protected Map<String, String>           mapOfFiles = new HashMap<>();

	@PostConstruct
	protected void init() {
		BeanScan.LOGGER.info("Starting scan at  {}",
				this.folder);
		try {
			this.listFiles(Paths.get(this.folder));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void listFiles(Path path) throws IOException {
		try (
				DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
			for (Path entry : stream) {
				if (!Files.isDirectory(entry)) {
					BeanScan.LOGGER.info("Processing file {}",
							entry);
					this.processFoundFile(path,
							entry);

				}
			}
		}
		try (
				DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {

			for (Path entry : stream) {
				if (Files.isDirectory(entry)) {
					this.listFiles(entry);
				}
			}
		}
	}

	public void processFoundFile(Path path, Path entry) {
		final String currentFileName = entry.getFileName().toString();
		String extension = currentFileName.substring(currentFileName.indexOf('.'),
				currentFileName.length());
		switch (extension.toUpperCase()) {
		case ".ARW": {
			final String absolutePathOfCurrentFile = entry.toAbsolutePath().toString();
			final String fileName = entry.toAbsolutePath().getFileName().toString();
			this.publishMainFile(absolutePathOfCurrentFile);
			this.mapOfFiles.put(absolutePathOfCurrentFile.toUpperCase(),
					fileName);
			break;
		}
		case ".ARW.COF":
		case ".ARW.COP": {
			Path parentPath = path.getParent().getParent().getParent();
			this.publishIfThereIsAMainFile(entry,
					currentFileName,
					parentPath);
			break;
		}
		case ".ARW.COS":
		case ".ARW.COMASK": {
			Path parentPath = path.getParent().getParent();
			this.publishIfThereIsAMainFile(entry,
					currentFileName,
					parentPath);
			break;
		}
		}
	}

	public void publishIfThereIsAMainFile(Path entry, final String currentFileName, Path parentPath) {
		String parentFileName = parentPath.toAbsolutePath().toString() + File.separatorChar
				+ currentFileName.substring(0,
						currentFileName.indexOf('.'))
				+ ".ARW";
		if (this.mapOfFiles.containsKey(parentFileName.toUpperCase())) {
			this.publishSubFile(parentFileName,
					entry.toAbsolutePath().toString());
		}
	}

	private void publishMainFile(String mainFile) {
		BeanScan.LOGGER.info("[EVENT][{}] publish main file ",
				mainFile);
		this.producerForPublishingOnStringTopic
				.send(new ProducerRecord<String, String>(this.outputTopic, mainFile, mainFile));
		this.producerForPublishingOnStringTopic.flush();
	}

	private void publishSubFile(String mainFile, String subFile) {
		BeanScan.LOGGER.info("[EVENT][{}] publish dependent file {}",
				mainFile,
				subFile);
		this.producerForPublishingOnStringTopic
				.send(new ProducerRecord<String, String>(this.outputTopic, subFile, subFile));
		this.producerForPublishingOnStringTopic
				.send(new ProducerRecord<String, String>(this.outputParentTopic, subFile, mainFile));
		this.producerForPublishingOnStringTopic.flush();
	}
}
