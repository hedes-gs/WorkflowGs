package com.gs.photo.common.workflow.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.URIException;

public class LocalFile extends AbstractRemoteFile {
    /**
    *
    */
    private static final long serialVersionUID = 1L;

    private final File        localFile;

    public LocalFile(File file) throws URIException, MalformedURLException {
        super(file.toURI()
            .toURL());
        this.localFile = file;
    }

    @Override
    public boolean canRead() { return this.localFile.canRead(); }

    @Override
    public boolean isDirectory() { return this.localFile.isDirectory(); }

    @Override
    public boolean isFile() { return this.localFile.isFile(); }

    @Override
    public AbstractRemoteFile[] listFiles() {
        List<AbstractRemoteFile> retValue;
        retValue = Arrays.asList(this.localFile.listFiles())
            .stream()
            .map((x) -> LocalFile.of(x))
            .collect(Collectors.toList());
        return retValue.toArray(new AbstractRemoteFile[retValue.size()]);
    }

    @Override
    public AbstractRemoteFile[] listFiles(FileFilter filter) {
        java.io.FileFilter nfsFilter = pathName -> filter.accept(LocalFile.of(pathName));
        List<AbstractRemoteFile> retValue;
        retValue = Arrays.asList(this.localFile.listFiles(nfsFilter))
            .stream()
            .map((x) -> LocalFile.of(x))
            .collect(Collectors.toList());
        return retValue.toArray(new AbstractRemoteFile[retValue.size()]);
    }

    @Override
    public boolean mkdir() { return this.localFile.mkdir(); }

    @Override
    public boolean canWrite() { // TODO Auto-generated method stub
        return this.localFile.canWrite();
    }

    @Override
    public boolean isHidden() { return false; }

    @Override
    public String getName() { return this.localFile.getName(); }

    @Override
    public boolean exists() { // TODO Auto-generated method stub
        return this.localFile.exists();
    }

    public static AbstractRemoteFile of(File x) {
        try {
            return new LocalFile(x);
        } catch (
            URIException |
            MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AbstractRemoteFile getParentFile() throws MalformedURLException, IOException {
        return LocalFile.of(this.localFile.getParentFile());
    }

    @Override
    public AbstractRemoteFile getChild(String fileName) {
        List<File> retValue = Arrays.asList(
            this.localFile.listFiles(
                (f) -> f.getName()
                    .equalsIgnoreCase(fileName)));
        if (retValue.size() == 1) { return LocalFile.of(retValue.get(0)); }
        return null;
    }

    @Override
    public InputStream openInputStream() throws IOException { return new FileInputStream(this.localFile); }

}
