package com.gs.photos.workflow.extimginfo.metadata;

public enum MetadataType {
	EXIF, // EXIF
	IPTC, // IPTC
	ICC_PROFILE, // ICC Profile
	XMP, // Adobe XMP
	PHOTOSHOP_IRB, // PHOTOSHOP Image Resource Block
	PHOTOSHOP_DDB, // PHOTOSHOP Document Data Block
	COMMENT, // General comment
	IMAGE, // Image specific information
	JPG_JFIF, // JPEG APP0 (JFIF)
	JPG_DUCKY, // JPEG APP12 (DUCKY)
	JPG_ADOBE, // JPEG APP14 (ADOBE)
	PNG_TEXTUAL, // PNG textual information
	PNG_TIME; // PNG tIME (last modified time) chunk
}