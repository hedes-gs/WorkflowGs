package com.workflow.model;

import lombok.Getter;
import lombok.Setter;

@HbaseTableName("image_project")
@Setter
@Getter
public class HbaseImageImportProject extends HbaseData {

	private static final long serialVersionUID = 1L;

	// Row key
	@Column(hbaseName = "import_date", isPartOfRowkey = true, rowKeyNumber = 0, toByte = ToByteLong.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMAGE_ID)
	protected long importDate;
	@Column(hbaseName = "import_id", isPartOfRowkey = true, rowKeyNumber = 1, toByte = ToByteString.class, fixedWidth = ModelConstants.FIXED_WIDTH_IMPORT_ID)
	protected String importId;

	// Data

	@Column(hbaseName = "image_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 100)
	protected String imageName = "";
	@Column(hbaseName = "thumb_name", toByte = ToByteString.class, columnFamily = "img", rowKeyNumber = 101)
	protected String thumbName = "";
	@Column(hbaseName = "path", rowKeyNumber = 102, toByte = ToByteString.class, columnFamily = "img")
	protected String path = "";
	@Column(hbaseName = "width", rowKeyNumber = 103, toByte = ToByteLong.class, columnFamily = "sz")
	protected long width;
	@Column(hbaseName = "height", rowKeyNumber = 104, toByte = ToByteLong.class, columnFamily = "sz")
	protected long height;

}
