package com.gs.photo.common.workflow.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Streams;

import jcifs.CIFSException;
import jcifs.SmbResource;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileFilter;

public class SmbResourceFile extends AbstractRemoteFile {

    protected static Logger   LOGGER           = LoggerFactory.getLogger(SmbResourceFile.class);
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private SmbResource       smbFile;

    public SmbResourceFile(SmbResource file) throws URISyntaxException {
        super(((SmbFile) file).getURL());
        this.smbFile = file;
    }

    @Override
    public boolean canRead() {
        try {
            return this.smbFile.canRead();
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean canWrite() {
        try {
            return this.smbFile.canWrite();
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isDirectory() {
        try {
            return this.smbFile.isDirectory();
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isFile() {
        try {
            return this.smbFile.isFile();
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isHidden() {
        try {
            return this.smbFile.isHidden();
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AbstractRemoteFile[] listFiles() {
        try {
            List<AbstractRemoteFile> files = Arrays.asList(((SmbFile) this.smbFile).listFiles())
                .stream()
                .map((x) -> SmbResourceFile.of(x))
                .collect(Collectors.toList());
            return files.toArray(new AbstractRemoteFile[files.size()]);
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

    public static AbstractRemoteFile of(SmbFile x) {
        try {
            return new SmbResourceFile(x);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static AbstractRemoteFile of(URLConnection x) {
        try {
            return new SmbResourceFile((SmbFile) x);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AbstractRemoteFile[] listFiles(FileFilter filter) {
        SmbFileFilter smbFilter = file -> filter.accept(SmbResourceFile.of(file));
        List<AbstractRemoteFile> files;
        SmbResourceFile.LOGGER.info("listFiles for file {} ", this.smbFile);
        try {
            files = Arrays.asList(((SmbFile) this.smbFile).listFiles(smbFilter))
                .stream()
                .map((x) -> SmbResourceFile.of(x))
                .collect(Collectors.toList());
            return files.toArray(new AbstractRemoteFile[files.size()]);
        } catch (SmbException e) {
            throw new RuntimeException(e);
        }

    }

    public AbstractRemoteFile[] listFiles(String wildcard) throws SmbException {
        List<AbstractRemoteFile> files;
        try {
            files = Arrays.asList(((SmbFile) this.smbFile).listFiles(wildcard))
                .stream()
                .map((x) -> SmbResourceFile.of(x))
                .collect(Collectors.toList());
            return files.toArray(new AbstractRemoteFile[files.size()]);
        } catch (SmbException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean mkdir() {
        try {
            ((SmbFile) this.smbFile).mkdir();
            return true;
        } catch (SmbException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream openInputStream() throws IOException { return ((SmbFile) this.smbFile).openInputStream(); }

    @Override
    public String getName() { return this.smbFile.getName(); }

    @Override
    public boolean exists() {
        try {
            return this.smbFile.exists();
        } catch (CIFSException e) {
            return false;
        }
    }

    @Override
    public AbstractRemoteFile getParentFile() throws MalformedURLException, IOException {
        return SmbResourceFile.of(new URL(((SmbFile) this.smbFile).getParent()).openConnection());
    }

    @Override
    public AbstractRemoteFile getChild(String fileName) {
        try {
            List<SmbResource> retValue = Streams.stream(
                this.smbFile.children(
                    (f) -> f.getName()
                        .equalsIgnoreCase(fileName)))
                .collect(Collectors.toList());
            if (retValue.size() == 1) { return SmbResourceFile.of((SmbFile) retValue.get(0)); }
            return null;
        } catch (CIFSException e) {
            throw new RuntimeException(e);
        }
    }

}
