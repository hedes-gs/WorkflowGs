package com.gs.photo.workflow.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.gs.photo.workflow.IServicesFile;

@Service
public class BeanServicesFile implements IServicesFile {

	private static final String FOLDER = "FOLDER_";

	protected static Logger LOGGER = LoggerFactory.getLogger(
		IServicesFile.class);

	protected static final DecimalFormat FORMATTER = new DecimalFormat("00000000");

	@Value("${copy.maxNumberOfFilesInAFolder}")
	protected int maxNumberOfFilesInAFolder;

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.gs.photos.impl.IServicesFile#getCurrentFolderInWhichCopyShouldBeDone(java
	 * .nio.file.Path)
	 */
	@Override
	public String getCurrentFolderInWhichCopyShouldBeDone(Path repositoryPath) throws IOException {
		Path currentLink = repositoryPath.resolve(
			"current_folder");
		if (Files.exists(
			currentLink)) {
			if (Files.isSymbolicLink(
				currentLink)) {
				File[] nbOfFiles = currentLink.toFile().listFiles();
				if (nbOfFiles == null || nbOfFiles.length < maxNumberOfFilesInAFolder) {
					return currentLink.toAbsolutePath().toString();
				}
				try {
					Files.delete(
						currentLink);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(
			repositoryPath)) {
			int currentEntryNb = -1;
			for (Path entry : stream) {
				if (Files.isDirectory(
					entry)) {
					LOGGER.info(
						"Processing folder {}",
						entry);
					int entryNb = getEntryNumber(
						entry);
					currentEntryNb = Math.max(
						currentEntryNb,
						entryNb);
				}
			}
			currentEntryNb++;
			Path newFolder = createNewFolderPathForCopying(
				repositoryPath,
				currentEntryNb);
			currentLink = Files.createSymbolicLink(
				currentLink,
				newFolder);
		} catch (IOException e) {
			LOGGER.warn(
				"Error on {} , {}",
				repositoryPath,
				e);
		}
		return currentLink.toRealPath().toAbsolutePath().toString();
	}

	// Should return somethnig like "FOLDER_0000001"
	private Path createNewFolderPathForCopying(Path repositoryPath2, int currentEntryNb) throws IOException {
		StringBuffer outPut = new StringBuffer(FOLDER);
		outPut.append(
			FORMATTER.format(
				currentEntryNb));
		final Path resolve = repositoryPath2.resolve(
			Paths.get(
				outPut.toString()));
		Files.createDirectory(
			resolve);
		return resolve;
	}

	private int getEntryNumber(Path entry) {
		String lastName = entry.getFileName().toString();
		if (lastName.startsWith(
			FOLDER)) {
			try {
				return Integer.parseInt(
					lastName.substring(
						FOLDER.length()));
			} catch (Exception e) {
				LOGGER.warn(
					"Error on {} , {}",
					entry,
					e);
			}
		}
		return 0;
	}
}
