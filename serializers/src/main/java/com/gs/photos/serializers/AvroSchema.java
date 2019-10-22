package com.gs.photos.serializers;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;

public class AvroSchema {
	static public Schema INSTANCE;
	static {
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("gs-avro-schema.avsc");
		Schema.Parser parser = new Schema.Parser();
		try {
			Schema schema = parser.parse(is);
			AvroSchema.INSTANCE = schema;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
