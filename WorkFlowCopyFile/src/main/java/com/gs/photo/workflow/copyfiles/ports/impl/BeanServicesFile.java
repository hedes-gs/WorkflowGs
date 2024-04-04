package com.gs.photo.workflow.copyfiles.ports.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.impl.FileUtils;
import com.gs.photo.workflow.copyfiles.config.SpecificApplicationProperties;
import com.gs.photo.workflow.copyfiles.ports.IServicesFile;
import com.workflow.model.files.FileToProcess;

@Service
public class BeanServicesFile implements IServicesFile {

    private static final String             FOLDER    = "FOLDER_";

    protected static Logger                 LOGGER    = LoggerFactory.getLogger(IServicesFile.class);

    protected static final DecimalFormat    FORMATTER = new DecimalFormat("00000000");

    @Autowired
    protected SpecificApplicationProperties specificApplicationProperties;

    @Override
    public void copyRemoteToLocal(FileToProcess file, File destFile) throws IOException {

        try (
            OutputStream os = new FileOutputStream(destFile)) {
            FileUtils.copyRemoteToLocal(file, os);
            destFile.setWritable(true, false);
            destFile.setReadable(true, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.gs.photos.impl.IServicesFile#getCurrentFolderInWhichCopyShouldBeDone(java
     * .nio.file.Path)
     */
    @Override
    public Path getCurrentFolderInWhichCopyShouldBeDone(Path repositoryPath) throws IOException {
        Path currentLink = repositoryPath.resolve("current_folder");
        if (Files.exists(currentLink)) {
            if (Files.isSymbolicLink(currentLink)) {
                File[] nbOfFiles = currentLink.toFile()
                    .listFiles();
                if ((nbOfFiles == null)
                    || (nbOfFiles.length < this.specificApplicationProperties.getMaxNumberOfFilesInAFolder())) {
                    return currentLink.toAbsolutePath();
                }
                try {
                    Files.delete(currentLink);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try (
            DirectoryStream<Path> stream = Files.newDirectoryStream(repositoryPath)) {
            int currentEntryNb = -1;
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    BeanServicesFile.LOGGER.info("Processing folder {}", entry);
                    int entryNb = this.getEntryNumber(entry);
                    currentEntryNb = Math.max(currentEntryNb, entryNb);
                }
            }
            currentEntryNb++;
            Path newFolder = this.createNewFolderPathForCopying(repositoryPath, currentEntryNb);
            currentLink = Files.createSymbolicLink(currentLink, newFolder);
        } catch (IOException e) {
            BeanServicesFile.LOGGER.warn("Error on {} , {}", repositoryPath, e);
        }
        return currentLink.toRealPath()
            .toAbsolutePath();
    }

    // Should return somethnig like "FOLDER_0000001"
    private Path createNewFolderPathForCopying(Path repositoryPath2, int currentEntryNb) throws IOException {
        StringBuffer outPut = new StringBuffer(BeanServicesFile.FOLDER);
        outPut.append(BeanServicesFile.FORMATTER.format(currentEntryNb));
        Path resolve = repositoryPath2.resolve(Paths.get(outPut.toString()));
        resolve = Files.createDirectory(resolve);
        final File newFolder = resolve.toFile();
        newFolder.setReadable(true, false);
        newFolder.setWritable(true, false);
        return resolve;
    }

    private int getEntryNumber(Path entry) {
        String lastName = entry.getFileName()
            .toString();
        if (lastName.startsWith(BeanServicesFile.FOLDER)) {
            try {
                return Integer.parseInt(lastName.substring(BeanServicesFile.FOLDER.length()));
            } catch (Exception e) {
                BeanServicesFile.LOGGER.warn("Error on {} , {}", entry, e);
            }
        }
        return 0;
    }
}
