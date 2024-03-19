package com.gs.photos.serializers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.workflow.model.HbaseData;
import com.workflow.model.HbaseImageThumbnail;

public abstract class AbstractModelSerializerAndDeserializer<T extends HbaseData> {
    protected static Logger                         LOGGER = LoggerFactory
        .getLogger(AbstractModelSerializerAndDeserializer.class);
    protected final Schema                          schema;
    protected final DatumWriter<T>                  writer;
    private ReflectDatumReader<HbaseImageThumbnail> reader;

    protected AbstractModelSerializerAndDeserializer() {
        this.schema = AvroSchema.INSTANCE;
        this.writer = new ReflectDatumWriter<>(this.schema);
        this.reader = new ReflectDatumReader<HbaseImageThumbnail>(this.schema);
    }

    protected byte[] toBytesGeneric(String topic, final T data) {
        final ByteArrayOutputStream bout = new ByteArrayOutputStream();
        final BinaryEncoder binEncoder = EncoderFactory.get()
            .binaryEncoder(bout, null);

        try {

            try {
                this.writer.write(data, binEncoder);
                binEncoder.flush();
            } catch (final Exception e) {
                AbstractModelSerializerAndDeserializer.LOGGER.error(
                    "Unexpected error while coding msg : {} , issue is : {} ",
                    data,
                    ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        } catch (Exception e) {
            AbstractModelSerializerAndDeserializer.LOGGER.error(
                "Unexpected error while decoding byte msg : {}, topic is {}, issue is : {} ",
                data,
                topic,
                ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
        return bout.toByteArray();
    }

    protected T fromBytesGeneric(String topic, byte[] t) {
        T retValue = null;
        if ((t != null) & (t.length > 0)) {
            try {
                final ByteArrayInputStream bais = new ByteArrayInputStream(t);
                final BinaryDecoder binDecoder = DecoderFactory.get()
                    .binaryDecoder(bais, null);
                Object cv = null;
                try {
                    cv = this.reader.read(null, binDecoder);
                    retValue = (T) cv;
                } catch (final Exception e) {
                    AbstractModelSerializerAndDeserializer.LOGGER.error(
                        "Unexpected error while decoding msg : {} , issue is : {} ",
                        cv,
                        ExceptionUtils.getStackTrace(e));
                    throw new RuntimeException(e);
                }
            } catch (Exception e) {
                AbstractModelSerializerAndDeserializer.LOGGER.error(
                    "Unexpected error while decoding byte msg : {}, topic is {}, issue is : {} ",
                    Arrays.toString(t),
                    topic,
                    ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
        }
        return retValue;
    }

    public void configure(Map<String, ?> configs, boolean isKey) {}

    public void close() {}

}
