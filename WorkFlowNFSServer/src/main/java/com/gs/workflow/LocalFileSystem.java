package com.gs.workflow;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.DosFileAttributes;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.UserPrincipal;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.nfs.FsExport;
import org.dcache.nfs.status.ExistException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NotEmptyException;
import org.dcache.nfs.status.NotSuppException;
import org.dcache.nfs.status.PermException;
import org.dcache.nfs.status.ServerFaultException;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

/**
 *
 */
public class LocalFileSystem implements VirtualFileSystem {

    protected static Logger                  LOGGER         = LoggerFactory.getLogger(LocalFileSystem.class);

    private final Path                       _root;
    private final Map<Long, Path>            inodeToPath    = new ConcurrentHashMap<>();
    private final Map<Path, Long>            pathToInode    = new ConcurrentHashMap<>();
    private final AtomicLong                 fileId         = new AtomicLong(1);                             // numbering
                                                                                                             // starts
                                                                                                             // at 1
    private final NfsIdMapping               _idMapper      = new SimpleIdMap();
    private final UserPrincipalLookupService _lookupService = FileSystems.getDefault()
        .getUserPrincipalLookupService();

    private final static boolean             IS_UNIX;
    static {
        IS_UNIX = !System.getProperty("os.name")
            .startsWith("Win");
    }

    private Inode toFh(long inodeNumber) { return Inode.forFile(Longs.toByteArray(inodeNumber)); }

    private long getInodeNumber(Inode inode) { return Longs.fromByteArray(inode.getFileId()); }

    private Path resolveInode(long inodeNumber) throws NoEntException {
        Path path = this.inodeToPath.get(inodeNumber);
        if (path == null) {
            LocalFileSystem.LOGGER.error("ERROR : Unable to find {} in inodePath {}", inodeNumber, this.inodeToPath);
            throw new NoEntException("inode #" + inodeNumber);
        }
        return path;
    }

    private long resolvePath(Path path) throws NoEntException {
        try {
            LocalFileSystem.LOGGER.info("Resolve path {} : {}", path, path.toRealPath());
            path = path.toRealPath();
            Long inodeNumber = this.pathToInode.get(path);
            if (inodeNumber == null) {
                if (Files.isRegularFile(path)) {
                    synchronized (this) {
                        inodeNumber = this.pathToInode.get(path);
                        if (inodeNumber == null) {
                            inodeNumber = this.fileId.getAndIncrement();
                            LocalFileSystem.LOGGER
                                .info("resolvePath new inodeNymber is {} - path is {} ", inodeNumber, path);

                            LocalFileSystem.this.map(inodeNumber, path);
                        }
                    }
                } else {
                    LocalFileSystem.LOGGER
                        .error("ERROR : path {} is not a regular file - pathToInode  {}", path, this.pathToInode);
                    throw new NoEntException("path " + path);
                }

            }
            return inodeNumber;
        } catch (IOException e) {
            LocalFileSystem.LOGGER
                .error("ERROR : path {} is not a regular file - pathToInode {} ", path, this.pathToInode);
            throw new NoEntException("path " + path, e);
        }
    }

    private void map(long inodeNumber, Path path) {
        if (this.inodeToPath.putIfAbsent(inodeNumber, path) != null) { throw new IllegalStateException(); }
        Long otherInodeNumber = this.pathToInode.putIfAbsent(path, inodeNumber);
        if (otherInodeNumber != null) {
            // try rollback
            if (this.inodeToPath.remove(inodeNumber) != path) {
                throw new IllegalStateException("cant map, rollback failed");
            }
            throw new IllegalStateException("path " + path + " / " + inodeNumber);
        }
        LocalFileSystem.LOGGER.info(" Map {} with {} ", path, inodeNumber);

    }

    private void unmap(long inodeNumber, Path path) {
        Path removedPath = this.inodeToPath.remove(inodeNumber);
        if (!path.equals(removedPath)) { throw new IllegalStateException(); }
        if (this.pathToInode.remove(path) != inodeNumber) { throw new IllegalStateException(); }
    }

    private void remap(long inodeNumber, Path oldPath, Path newPath) {
        // TODO - attempt rollback?
        this.unmap(inodeNumber, oldPath);
        this.map(inodeNumber, newPath);
    }

    public LocalFileSystem(
        Path root,
        Iterable<FsExport> exportIterable
    ) throws IOException {
        SimpleNfsServer.LOGGER.info(" Create LocalFileSystem ...");
        this._root = root;
        Collection<Path> subDir = new HashSet<>();
        assert (Files.exists(this._root));
        for (FsExport export : exportIterable) {
            String relativeExportPath = export.getPath()
                .substring(1); // remove the opening '/'
            Path exportRootPath = root.resolve(relativeExportPath);
            if (!Files.exists(exportRootPath)) {
                Files.createDirectories(exportRootPath);
            }
            subDir.add(exportRootPath);
        }
        LocalFileSystem.LOGGER.info(" subDir  are {}", subDir);
        // map existing structure (if any)
        this.map(this.fileId.getAndIncrement(), this._root); // so root is always inode #1
        subDir.forEach((f) -> this.buildInodes(f));
    }

    protected void buildInodes(Path f) {
        try {
            Files.walkFileTree(f, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    try {
                        FileVisitResult superRes = super.preVisitDirectory(dir, attrs);
                        if (superRes != FileVisitResult.CONTINUE) { return superRes; }
                        if (dir.equals(LocalFileSystem.this._root)) { return FileVisitResult.CONTINUE; }
                        LocalFileSystem.this.map(LocalFileSystem.this.fileId.getAndIncrement(), dir);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    try {
                        FileVisitResult superRes = super.visitFile(file, attrs);
                        if (superRes != FileVisitResult.CONTINUE) { return superRes; }
                        LocalFileSystem.this.map(LocalFileSystem.this.fileId.getAndIncrement(), file);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Inode create(Inode parent, Type type, String path, Subject subject, int mode) throws IOException {
        LocalFileSystem.LOGGER.info("create inode");

        long parentInodeNumber = this.getInodeNumber(parent);
        Path parentPath = this.resolveInode(parentInodeNumber);
        Path newPath = parentPath.resolve(path);
        try {
            Files.createFile(newPath);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("path " + newPath);
        }
        long newInodeNumber = this.fileId.getAndIncrement();
        this.map(newInodeNumber, newPath);
        this.setOwnershipAndMode(newPath, subject, mode);
        return this.toFh(newInodeNumber);
    }

    @Override
    public FsStat getFsStat() throws IOException {
        FileStore store = Files.getFileStore(this._root);
        long total = store.getTotalSpace();
        long free = store.getUsableSpace();
        return new FsStat(total, Long.MAX_VALUE, total - free, this.pathToInode.size());
    }

    @Override
    public Inode getRootInode() throws IOException {
        return this.toFh(1); // always #1 (see constructor)
    }

    @Override
    public Inode lookup(Inode parent, String path) throws IOException {
        LocalFileSystem.LOGGER.info("lookup parent is {} - path is {} ", parent, path);

        // TODO - several issues
        // 2. we might accidentally allow composite paths here ("/dome/dir/down")
        // 3. we dont actually check that the parent exists
        long parentInodeNumber = this.getInodeNumber(parent);
        Path parentPath = this.resolveInode(parentInodeNumber);
        Path child;
        if (path.equals(".")) {
            child = parentPath;
        } else if (path.equals("..")) {
            child = parentPath.getParent();
        } else {
            child = parentPath.resolve(path);
        }
        long childInodeNumber = this.resolvePath(child);
        return this.toFh(childInodeNumber);
    }

    @Override
    public Inode link(Inode parent, Inode existing, String target, Subject subject) throws IOException {
        long parentInodeNumber = this.getInodeNumber(parent);
        Path parentPath = this.resolveInode(parentInodeNumber);

        long existingInodeNumber = this.getInodeNumber(existing);
        Path existingPath = this.resolveInode(existingInodeNumber);

        Path targetPath = parentPath.resolve(target);

        try {
            Files.createLink(targetPath, existingPath);
        } catch (UnsupportedOperationException e) {
            throw new NotSuppException("Not supported", e);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("Path exists " + target, e);
        } catch (SecurityException e) {
            throw new PermException("Permission denied: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new ServerFaultException("Failed to create: " + e.getMessage(), e);
        }

        long newInodeNumber = this.fileId.getAndIncrement();
        this.map(newInodeNumber, targetPath);
        return this.toFh(newInodeNumber);
    }

    @Override
    public DirectoryStream list(Inode inode, byte[] bytes, long l) throws IOException {
        long inodeNumber = this.getInodeNumber(inode);
        Path path = this.resolveInode(inodeNumber);
        final List<DirectoryEntry> list = new ArrayList<>();
        try (
            java.nio.file.DirectoryStream<Path> ds = Files.newDirectoryStream(path)) {
            int cookie = 2; // first allowed cookie
            for (Path p : ds) {
                cookie++;
                if (cookie > l) {
                    long ino = this.resolvePath(p);
                    list.add(
                        new DirectoryEntry(p.getFileName()
                            .toString(), this.toFh(ino), this.statPath(p, ino), cookie));
                }
            }
        }
        return new DirectoryStream(list);
    }

    @Override
    public byte[] directoryVerifier(Inode inode) throws IOException { return DirectoryStream.ZERO_VERIFIER; }

    @Override
    public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
        long parentInodeNumber = this.getInodeNumber(parent);
        Path parentPath = this.resolveInode(parentInodeNumber);
        Path newPath = parentPath.resolve(path);
        try {
            Files.createDirectory(newPath);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("path " + newPath);
        }
        long newInodeNumber = this.fileId.getAndIncrement();
        this.map(newInodeNumber, newPath);
        this.setOwnershipAndMode(newPath, subject, mode);
        return this.toFh(newInodeNumber);
    }

    private void setOwnershipAndMode(Path target, Subject subject, int mode) {
        if (!LocalFileSystem.IS_UNIX) {
            // FIXME: windows must support some kind of file owhership as well
            return;
        }

        int uid = -1;
        int gid = -1;
        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                uid = (int) ((UidPrincipal) principal).getUid();
            }
            if (principal instanceof GidPrincipal) {
                gid = (int) ((GidPrincipal) principal).getGid();
            }
        }

        if (uid != -1) {
            try {
                Files.setAttribute(target, "unix:uid", uid, NOFOLLOW_LINKS);
            } catch (IOException e) {
                LocalFileSystem.LOGGER.warn("Unable to chown file {}: {}", target, e.getMessage());
            }
        } else {
            LocalFileSystem.LOGGER.warn("File created without uid: {}", target);
        }
        if (gid != -1) {
            try {
                Files.setAttribute(target, "unix:gid", gid, NOFOLLOW_LINKS);
            } catch (IOException e) {
                LocalFileSystem.LOGGER.warn("Unable to chown file {}: {}", target, e.getMessage());
            }
        } else {
            LocalFileSystem.LOGGER.warn("File created without gid: {}", target);
        }

        try {
            Files.setAttribute(target, "unix:mode", mode, NOFOLLOW_LINKS);
        } catch (IOException e) {
            LocalFileSystem.LOGGER.warn("Unable to set mode of file {}: {}", target, e.getMessage());
        }
    }

    @Override
    public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
        // TODO - several issues
        // 1. we might not deal with "." and ".." properly
        // 2. we might accidentally allow composite paths here ("/dome/dir/down")
        // 3. we return true (changed) even though in theory a file might be renamed to
        // itself?
        long currentParentInodeNumber = this.getInodeNumber(src);
        Path currentParentPath = this.resolveInode(currentParentInodeNumber);
        long destParentInodeNumber = this.getInodeNumber(dest);
        Path destPath = this.resolveInode(destParentInodeNumber);
        Path currentPath = currentParentPath.resolve(oldName);
        long targetInodeNumber = this.resolvePath(currentPath);
        Path newPath = destPath.resolve(newName);
        try {
            Files.move(currentPath, newPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("path " + newPath);
        }
        this.remap(targetInodeNumber, currentPath, newPath);
        return true;
    }

    @Override
    public Inode parentOf(Inode inode) throws IOException {
        long inodeNumber = this.getInodeNumber(inode);
        if (inodeNumber == 1) {
            throw new NoEntException("no parent"); // its the root
        }
        Path path = this.resolveInode(inodeNumber);
        Path parentPath = path.getParent();
        long parentInodeNumber = this.resolvePath(parentPath);
        return this.toFh(parentInodeNumber);
    }

    @Override
    public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
        long inodeNumber = this.getInodeNumber(inode);
        Path path = this.resolveInode(inodeNumber);
        ByteBuffer destBuffer = ByteBuffer.wrap(data, 0, count);
        try (
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return channel.read(destBuffer, offset);
        }
    }

    @Override
    public String readlink(Inode inode) throws IOException {
        long inodeNumber = this.getInodeNumber(inode);
        Path path = this.resolveInode(inodeNumber);
        return Files.readSymbolicLink(path)
            .toString();
    }

    @Override
    public void remove(Inode parent, String path) throws IOException {
        long parentInodeNumber = this.getInodeNumber(parent);
        Path parentPath = this.resolveInode(parentInodeNumber);
        Path targetPath = parentPath.resolve(path);
        long targetInodeNumber = this.resolvePath(targetPath);
        try {
            Files.delete(targetPath);
        } catch (DirectoryNotEmptyException e) {
            throw new NotEmptyException("dir " + targetPath + " is note empty", e);
        }
        this.unmap(targetInodeNumber, targetPath);
    }

    @Override
    public Inode symlink(Inode parent, String linkName, String targetName, Subject subject, int mode)
        throws IOException {
        long parentInodeNumber = this.getInodeNumber(parent);
        Path parentPath = this.resolveInode(parentInodeNumber);
        Path link = parentPath.resolve(linkName);
        Path target = parentPath.resolve(targetName);
        if (!targetName.startsWith("/")) {
            target = parentPath.relativize(target);
        }
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException e) {
            throw new NotSuppException("Not supported", e);
        } catch (FileAlreadyExistsException e) {
            throw new ExistException("Path exists " + linkName, e);
        } catch (SecurityException e) {
            throw new PermException("Permission denied: " + e.getMessage(), e);
        } catch (IOException e) {
            throw new ServerFaultException("Failed to create: " + e.getMessage(), e);
        }

        this.setOwnershipAndMode(link, subject, mode);

        long newInodeNumber = this.fileId.getAndIncrement();
        this.map(newInodeNumber, link);
        return this.toFh(newInodeNumber);
    }

    @Override
    public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel)
        throws IOException {
        long inodeNumber = this.getInodeNumber(inode);
        Path path = this.resolveInode(inodeNumber);
        ByteBuffer srcBuffer = ByteBuffer.wrap(data, 0, count);
        try (
            FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            int bytesWritten = channel.write(srcBuffer, offset);
            return new WriteResult(StabilityLevel.FILE_SYNC, bytesWritten);
        }
    }

    @Override
    public void commit(Inode inode, long l, int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private Stat statPath(Path p, long inodeNumber) throws IOException {

        Class<? extends BasicFileAttributeView> attributeClass = LocalFileSystem.IS_UNIX ? PosixFileAttributeView.class
            : DosFileAttributeView.class;

        BasicFileAttributes attrs = Files.getFileAttributeView(p, attributeClass, NOFOLLOW_LINKS)
            .readAttributes();

        Stat stat = new Stat();

        stat.setATime(
            attrs.lastAccessTime()
                .toMillis());
        stat.setCTime(
            attrs.creationTime()
                .toMillis());
        stat.setMTime(
            attrs.lastModifiedTime()
                .toMillis());

        if (LocalFileSystem.IS_UNIX) {
            stat.setGid((Integer) Files.getAttribute(p, "unix:gid", NOFOLLOW_LINKS));
            stat.setUid((Integer) Files.getAttribute(p, "unix:uid", NOFOLLOW_LINKS));
            stat.setMode((Integer) Files.getAttribute(p, "unix:mode", NOFOLLOW_LINKS));
            stat.setNlink((Integer) Files.getAttribute(p, "unix:nlink", NOFOLLOW_LINKS));
        } else {
            DosFileAttributes dosAttrs = (DosFileAttributes) attrs;
            stat.setGid(0);
            stat.setUid(1000);
            int type = dosAttrs.isSymbolicLink() ? Stat.S_IFLNK : dosAttrs.isDirectory() ? Stat.S_IFDIR : Stat.S_IFREG;
            stat.setMode(type | (dosAttrs.isReadOnly() ? 0400 : 0600));
            stat.setNlink(1);
        }

        stat.setDev(17);
        stat.setIno((int) inodeNumber);
        stat.setRdev(17);
        stat.setSize(attrs.size());
        stat.setFileid((int) inodeNumber);
        stat.setGeneration(
            attrs.lastModifiedTime()
                .toMillis());

        return stat;
    }

    @Override
    public int access(Inode inode, int mode) throws IOException { return mode; }

    @Override
    public Stat getattr(Inode inode) throws IOException {
        long inodeNumber = this.getInodeNumber(inode);
        Path path = this.resolveInode(inodeNumber);
        return this.statPath(path, inodeNumber);

    }

    @Override
    public void setattr(Inode inode, Stat stat) throws IOException {
        if (!LocalFileSystem.IS_UNIX) {
            // FIXME: windows must support some kind of attribute update as well
            return;
        }

        long inodeNumber = this.getInodeNumber(inode);
        Path path = this.resolveInode(inodeNumber);
        PosixFileAttributeView attributeView = Files
            .getFileAttributeView(path, PosixFileAttributeView.class, NOFOLLOW_LINKS);
        if (stat.isDefined(Stat.StatAttribute.OWNER)) {
            try {
                String uid = String.valueOf(stat.getUid());
                UserPrincipal user = this._lookupService.lookupPrincipalByName(uid);
                attributeView.setOwner(user);
            } catch (IOException e) {
                throw new UnsupportedOperationException("set uid failed: " + e.getMessage(), e);
            }
        }
        if (stat.isDefined(Stat.StatAttribute.GROUP)) {
            try {
                String gid = String.valueOf(stat.getGid());
                GroupPrincipal group = this._lookupService.lookupPrincipalByGroupName(gid);
                attributeView.setGroup(group);
            } catch (IOException e) {
                throw new UnsupportedOperationException("set gid failed: " + e.getMessage(), e);
            }
        }
        if (stat.isDefined(Stat.StatAttribute.MODE)) {
            try {
                Files.setAttribute(path, "unix:mode", stat.getMode(), NOFOLLOW_LINKS);
            } catch (IOException e) {
                throw new UnsupportedOperationException("set mode unsupported: " + e.getMessage(), e);
            }
        }
        if (stat.isDefined(Stat.StatAttribute.SIZE)) {
            try (
                RandomAccessFile raf = new RandomAccessFile(path.toFile(), "rw")) {
                raf.setLength(stat.getSize());
            }
        }
        if (stat.isDefined(Stat.StatAttribute.ATIME)) {
            try {
                FileTime time = FileTime.fromMillis(stat.getCTime());
                Files.setAttribute(path, "unix:lastAccessTime", time, NOFOLLOW_LINKS);
            } catch (IOException e) {
                throw new UnsupportedOperationException("set atime failed: " + e.getMessage(), e);
            }
        }
        if (stat.isDefined(Stat.StatAttribute.MTIME)) {
            try {
                FileTime time = FileTime.fromMillis(stat.getMTime());
                Files.setAttribute(path, "unix:lastModifiedTime", time, NOFOLLOW_LINKS);
            } catch (IOException e) {
                throw new UnsupportedOperationException("set mtime failed: " + e.getMessage(), e);
            }
        }
        if (stat.isDefined(Stat.StatAttribute.CTIME)) {
            try {
                FileTime time = FileTime.fromMillis(stat.getCTime());
                Files.setAttribute(path, "unix:ctime", time, NOFOLLOW_LINKS);
            } catch (IOException e) {
                throw new UnsupportedOperationException("set ctime failed: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public nfsace4[] getAcl(Inode inode) throws IOException { return new nfsace4[0]; }

    @Override
    public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
        // NOP
    }

    @Override
    public boolean hasIOLayout(Inode inode) throws IOException { return false; }

    @Override
    public AclCheckable getAclCheckable() { return AclCheckable.UNDEFINED_ALL; }

    @Override
    public NfsIdMapping getIdMapper() { return this._idMapper; }
}
