/* jcifs smb client library in Java
 * Copyright (C) 2000  "Michael B. Allen" <jcifs at samba dot org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package com.gs.photo.common.workflow.impl;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL handler for transparent smb:// URL handling
 *
 */
public class LocalFileHandler extends URLStreamHandler {

    private static final Logger log          = LoggerFactory.getLogger(LocalFileHandler.class);
    private static final int    DEFAULT_PORT = 2049;

    /**
     *
     */
    public LocalFileHandler() {}

    @Override
    protected int getDefaultPort() { return LocalFileHandler.DEFAULT_PORT; }

    @Override
    public URLConnection openConnection(URL u) throws IOException {
        return URLConnectionWrapper.of(new LocalFileURLConnection(u));
    }

}
