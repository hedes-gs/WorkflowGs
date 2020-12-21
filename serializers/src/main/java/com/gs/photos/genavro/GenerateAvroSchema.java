package com.gs.photos.genavro;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.reflect.ClassPath;

public class GenerateAvroSchema {
    public static void main(String[] args) {
        final ClassLoader loader = Thread.currentThread()
            .getContextClassLoader();

        try {

            List<Schema> schemas = new ArrayList<>();
            for (final ClassPath.ClassInfo info : ClassPath.from(loader)
                .getTopLevelClasses()) {
                if (info.getName()
                    .startsWith("com.workflow.model.events")
                    || info.getName()
                        .startsWith("com.workflow.model.Hbase")
                    || info.getName()
                        .startsWith("com.workflow.model.files")
                    || info.getName()
                        .startsWith("com.workflow.model.Exchange")
                    || info.getName()
                        .startsWith("com.workflow.model.Collection")
                    || info.getName()
                        .startsWith("com.workflow.model.storm")) {
                    final Class<?> clazz = info.load();

                    if (!clazz.isInterface()) {
                        try {
                            System.out.println("... adding " + clazz + " to avro schema");
                            Schema schema = ReflectData.get()
                                .getSchema(clazz);
                            schemas.add(schema);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                    }
                }
            }

            Schema schema = Schema.createUnion(schemas);
            String asJson = schema.toString(true);
            File f = new File("src/main/resources/gs-avro-schema.avsc");
            System.out.println("......... Dumping schema in " + f.getAbsolutePath());
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(f));
            dos.write(asJson.getBytes(Charset.forName("ISO-8859-1")));
            dos.close();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
