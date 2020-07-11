package com.gs.photos.serializers;

import java.io.File;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import com.workflow.model.events.ImportEvent;
import com.workflow.model.files.FileToProcess;

public class TestFileToProcessDeserializer {

    @Test
    public void test() {
        try (
            FileToProcessSerializer ser = new FileToProcessSerializer();
            FileToProcessDeserializer deser = new FileToProcessDeserializer()) {
            FileToProcess data = FileToProcess.builder()
                .withCompressedFile(true)
                .withDataId("MonFile.ARW")
                .withDataCreationDate(0)
                .withHost("IPC3")
                .withName("nom.arw")
                .withPath("/ici")
                .withImportEvent(
                    ImportEvent.builder()
                        .withAlbum("MyAlbum")
                        .withImportName("Mon import")
                        .withKeyWords(Arrays.asList("kw1", "kw2"))
                        .build())
                .build();
            byte[] serValue = ser.toBytesGeneric(data);
            FileToProcess data2 = deser.deserialize("topic", serValue);
            Assert.assertEquals(data, data2);
        }

    }

    @Test
    public void test2() {
        try (
            FileToProcessSerializer ser = new FileToProcessSerializer();
            FileToProcessDeserializer deser = new FileToProcessDeserializer()) {
            FileToProcess data = this.buildFileToProcess(new File("pom.xml"), false);
            byte[] serValue = ser.toBytesGeneric(data);
            FileToProcess data2 = deser.deserialize("topic", serValue);
            Assert.assertEquals(data, data2);
        }

    }

    private FileToProcess buildFileToProcess(File file, boolean isCompressed) {
        return FileToProcess.builder()
            .withDataId(file.getName())
            .withName(file.getName())
            .withHost("IPC3")
            .withPath(file.getAbsolutePath())
            .withCompressedFile(isCompressed)
            .withImportEvent(
                ImportEvent.builder()
                    .withAlbum("MyAlbum")
                    .withImportName("Mon import")
                    .withKeyWords(Arrays.asList("kw1", "kw2"))
                    .build())
            .build();
    }

}
