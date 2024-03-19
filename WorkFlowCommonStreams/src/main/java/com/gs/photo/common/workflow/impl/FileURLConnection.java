/*
 * Copyright (c) 1995, 2010, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/**
 * Open an file input stream given a URL.
 * @author      James Gosling
 * @author      Steven B. Byrne
 */

package com.gs.photo.common.workflow.impl;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilePermission;
import java.io.IOException;
import java.io.InputStream;
import java.net.FileNameMap;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.security.Permission;
import java.text.Collator;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class FileURLConnection extends URLConnection {

    static String CONTENT_LENGTH = "content-length";
    static String CONTENT_TYPE   = "content-type";
    static String TEXT_PLAIN     = "text/plain";
    static String LAST_MODIFIED  = "last-modified";

    String        contentType;
    InputStream   is;

    File          file;
    String        filename;
    boolean       isDirectory    = false;
    boolean       exists         = false;
    List<String>  files;

    long          length         = -1;
    long          lastModified   = 0;

    public FileURLConnection(
        URL u,
        File file
    ) {
        super(u);
        this.file = file;
    }

    /*
     * Note: the semantics of FileURLConnection object is that the results of the
     * various URLConnection calls, such as getContentType, getInputStream or
     * getContentLength reflect whatever was true when connect was called.
     */
    @Override
    public void connect() throws IOException {
        if (!this.connected) {
            try {
                this.filename = this.file.toString();
                this.isDirectory = this.file.isDirectory();
                if (this.isDirectory) {
                    String[] fileList = this.file.list();
                    if (fileList == null) {
                        throw new FileNotFoundException(this.filename + " exists, but is not accessible");
                    }
                    this.files = Arrays.<String>asList(fileList);
                } else {

                    this.is = new BufferedInputStream(new FileInputStream(this.filename));
                }
            } catch (IOException e) {
                throw e;
            }
            this.connected = true;
        }
    }

    private boolean initializedHeaders = false;

    private void initializeHeaders() {
        try {
            this.connect();
            this.exists = this.file.exists();
        } catch (IOException e) {
        }
        if (!this.initializedHeaders || !this.exists) {
            this.length = this.file.length();
            this.lastModified = this.file.lastModified();

            if (!this.isDirectory) {
                FileNameMap map = java.net.URLConnection.getFileNameMap();
                this.contentType = map.getContentTypeFor(this.filename);
                /*
                 * Format the last-modified field into the preferred Internet standard - ie:
                 * fixed-length subset of that defined by RFC 1123
                 */
                if (this.lastModified != 0) {
                    Date date = new Date(this.lastModified);
                    SimpleDateFormat fo = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
                    fo.setTimeZone(TimeZone.getTimeZone("GMT"));
                }
            }
            this.initializedHeaders = true;
        }
    }

    @Override
    public String getHeaderField(String name) {
        this.initializeHeaders();
        return super.getHeaderField(name);
    }

    @Override
    public String getHeaderField(int n) {
        this.initializeHeaders();
        return super.getHeaderField(n);
    }

    @Override
    public int getContentLength() {
        this.initializeHeaders();
        if (this.length > Integer.MAX_VALUE) { return -1; }
        return (int) this.length;
    }

    @Override
    public long getContentLengthLong() {
        this.initializeHeaders();
        return this.length;
    }

    @Override
    public String getHeaderFieldKey(int n) {
        this.initializeHeaders();
        return super.getHeaderFieldKey(n);
    }

    @Override
    public long getLastModified() {
        this.initializeHeaders();
        return this.lastModified;
    }

    @Override
    public synchronized InputStream getInputStream() throws IOException {

        int iconHeight;
        int iconWidth;

        this.connect();

        if (this.is == null) {
            if (this.isDirectory) {
                FileNameMap map = java.net.URLConnection.getFileNameMap();

                StringBuilder sb = new StringBuilder();

                if (this.files == null) { throw new FileNotFoundException(this.filename); }

                Collections.sort(this.files, Collator.getInstance());

                for (String fileName : this.files) {
                    sb.append(fileName);
                    sb.append("\n");
                }
                // Put it into a (default) locale-specific byte-stream.
                this.is = new ByteArrayInputStream(sb.toString()
                    .getBytes());
            } else {
                throw new FileNotFoundException(this.filename);
            }
        }
        return this.is;
    }

    Permission permission;

    /*
     * since getOutputStream isn't supported, only read permission is relevant
     */
    @Override
    public Permission getPermission() throws IOException {
        if (this.permission == null) {
            String decodedPath = FileURLConnection.decode(this.url.getPath());
            if (File.separatorChar == '/') {
                this.permission = new FilePermission(decodedPath, "read");
            } else {
                // decode could return /c:/x/y/z.
                if ((decodedPath.length() > 2) && (decodedPath.charAt(0) == '/') && (decodedPath.charAt(2) == ':')) {
                    decodedPath = decodedPath.substring(1);
                }
                this.permission = new FilePermission(decodedPath.replace('/', File.separatorChar), "read");
            }
        }
        return this.permission;
    }

    protected static String decode(String s) {
        int n = s.length();
        if ((n == 0) || (s.indexOf('%') < 0)) { return s; }

        StringBuilder sb = new StringBuilder(n);
        ByteBuffer bb = ByteBuffer.allocate(n);
        CharBuffer cb = CharBuffer.allocate(n);
        CharsetDecoder dec = Charset.forName("UTF-8")
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);

        char c = s.charAt(0);
        for (int i = 0; i < n;) {
            assert c == s.charAt(i);
            if (c != '%') {
                sb.append(c);
                if (++i >= n) {
                    break;
                }
                c = s.charAt(i);
                continue;
            }
            bb.clear();
            int ui = i;
            for (;;) {
                assert ((n - i) >= 2);
                try {
                    bb.put(FileURLConnection.unescape(s, i));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException();
                }
                i += 3;
                if (i >= n) {
                    break;
                }
                c = s.charAt(i);
                if (c != '%') {
                    break;
                }
            }
            bb.flip();
            cb.clear();
            dec.reset();
            CoderResult cr = dec.decode(bb, cb, true);
            if (cr.isError()) { throw new IllegalArgumentException("Error decoding percent encoded characters"); }
            cr = dec.flush(cb);
            if (cr.isError()) { throw new IllegalArgumentException("Error decoding percent encoded characters"); }
            sb.append(
                cb.flip()
                    .toString());
        }

        return sb.toString();
    }

    private static byte unescape(String s, int i) { return (byte) Integer.parseInt(s, i + 1, i + 3, 16); }

}
