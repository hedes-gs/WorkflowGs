/**
 * COPYRIGHT (C) 2014-2017 WEN YU (YUWEN_66@YAHOO.COM) ALL RIGHTS RESERVED.
 *
 * This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Any modifications to this file must keep this entire header intact.
 *
 * Change History - most recent changes go on top of previous changes
 *
 * ExifThumbnail.java
 *
 * Who   Date       Description
 * ====  =========  =================================================
 * WY    27Apr2015  Added copy constructor
 * WY    10Apr2015  Added new constructor, changed write()
 * WY    09Apr2015  Moved setWriteQuality() to super class
 * WY    14Jan2015  initial creation
 */

package com.gs.photos.workflow.metadata.exif;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;

import com.gs.photos.workflow.metadata.IFD;

public class ExifThumbnail extends Thumbnail {
	// Comprised of an IFD and an associated image
	// Create thumbnail IFD (IFD1 in the case of JPEG EXIF segment)
	private IFD thumbnailIFD = new IFD();

	public ExifThumbnail() {
	}

	public ExifThumbnail(BufferedImage thumbnail) {
		super(thumbnail);
	}

	public ExifThumbnail(ExifThumbnail other) { // Copy constructor
		this.dataType = other.dataType;
		this.height = other.height;
		this.width = other.width;
		this.thumbnail = other.thumbnail;
		this.compressedThumbnail = other.compressedThumbnail;
		this.thumbnailIFD = other.thumbnailIFD;
	}

	public ExifThumbnail(int width, int height, int dataType, byte[] compressedThumbnail) {
		super(width, height, dataType, compressedThumbnail);
	}

	public ExifThumbnail(int width, int height, int dataType, byte[] compressedThumbnail, IFD thumbnailIFD) {
		super(width, height, dataType, compressedThumbnail);
		this.thumbnailIFD = thumbnailIFD;
	}

	@Override
	public void write(OutputStream os) throws IOException {
		// TODO Auto-generated method stub

	}
}