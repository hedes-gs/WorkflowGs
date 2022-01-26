package com.gs.photo.common.workflow.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;

import com.emc.ecs.nfsclient.nfs.io.Nfs3File;
import com.emc.ecs.nfsclient.nfs.io.NfsFile;
import com.emc.ecs.nfsclient.nfs.io.NfsFileFilter;
import com.emc.ecs.nfsclient.nfs.io.NfsFileInputStream;
import com.emc.ecs.nfsclient.nfs.io.NfsFilenameFilter;

public class NFSFile extends AbstractRemoteFile {

    private final Nfs3File nfs3File;

    public NFSFile(NfsFile<?, ?> nfs3File) throws URIException, MalformedURLException {
        super(new URL("nfs://" + URIUtil.encodePath(nfs3File.getAbsolutePath())));
        this.nfs3File = (Nfs3File) nfs3File;
    }

    public boolean canDelete() throws IOException { return this.nfs3File.canDelete(); }

    public boolean canExtend() throws IOException { return this.nfs3File.canExtend(); }

    public boolean canModify() throws IOException { return this.nfs3File.canModify(); }

    @Override
    public String toString() { return "NFSFile [nfs3File=" + this.nfs3File + "]"; }

    @Override
    public void delete() {
        try {
            this.nfs3File.delete();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean canRead() {
        try {
            return this.nfs3File.canRead();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isDirectory() {
        try {
            return this.nfs3File.isDirectory();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public URL toExternalURL() { return this.getUrl(); }

    @Override
    public boolean isFile() {
        try {
            return this.nfs3File.isFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> list(NfsFilenameFilter filter) throws IOException { return this.nfs3File.list(filter); }

    @Override
    public AbstractRemoteFile[] listFiles() {
        List<AbstractRemoteFile> retValue;
        try {
            retValue = this.nfs3File.listFiles()
                .stream()
                .map((x) -> NFSFile.of(x))
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue.toArray(new AbstractRemoteFile[retValue.size()]);
    }

    @Override
    public AbstractRemoteFile[] listFiles(FileFilter filter) {
        NfsFileFilter nfsFilter = pathName -> filter.accept(NFSFile.of(pathName));
        List<AbstractRemoteFile> retValue;
        try {
            retValue = this.nfs3File.listFiles(nfsFilter)
                .stream()
                .map((x) -> NFSFile.of(x))
                .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue.toArray(new AbstractRemoteFile[retValue.size()]);
    }

    @Override
    public boolean mkdir() {
        try {
            this.nfs3File.mkdir();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public boolean canWrite() { // TODO Auto-generated method stub
        try {
            return this.nfs3File.canModify();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isHidden() { return false; }

    @Override
    public String getName() { return this.nfs3File.getName(); }

    @Override
    public boolean exists() { // TODO Auto-generated method stub
        try {
            return this.nfs3File.exists();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static AbstractRemoteFile of(NfsFile<?, ?> x) {
        try {
            return new NFSFile(x);
        } catch (
            URIException |
            MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public AbstractRemoteFile getParentFile() throws MalformedURLException, IOException {
        return NFSFile.of(new Nfs3File(this.nfs3File.getNfs(), this.nfs3File.getParent()));
    }

    @Override
    public AbstractRemoteFile getChild(String fileName) {
        try {
            List<Nfs3File> retValue = this.nfs3File.listFiles(
                (f) -> f.getName()
                    .equalsIgnoreCase(fileName));
            if (retValue.size() == 1) { return NFSFile.of(retValue.get(0)); }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream openInputStream() throws IOException {
        return new NfsFileInputStream(this.nfs3File, FileUtils.NB_OF_BYTES_ON_WHICH_KEY_IS_COMPUTED);
    }

}
