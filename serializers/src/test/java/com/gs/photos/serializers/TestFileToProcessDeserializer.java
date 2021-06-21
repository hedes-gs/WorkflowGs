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
                .withUrl("nfs://192.168.1.101/Public/Shared Pictures/vacances-2016/2016-07-17 12.28.07/_DSC8298.ARW")
                .withDataId("MonFile.ARW")
                .withDataCreationDate(0)
                .withName("nom.arw")
                .withImportEvent(
                    ImportEvent.builder()
                        .withAlbum("MyAlbum")
                        .withImportName("Mon import")
                        .withKeyWords(Arrays.asList("kw1", "kw2"))
                        .withScanFolder("Scan folder")
                        .build())
                .build();
            byte[] serValue = ser.toBytesGeneric("Mon topic", data);
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
            byte[] serValue = ser.toBytesGeneric("Mon topic", data);
            FileToProcess data2 = deser.deserialize("topic", serValue);
            Assert.assertEquals(data, data2);
        }

    }

    private FileToProcess buildFileToProcess(File file, boolean isCompressed) {
        return FileToProcess.builder()
            .withDataId(file.getName())
            .withName(file.getName())
            .withUrl("nfs://192.168.1.101/Public/Shared Pictures/vacances-2016/2016-07-17 12.28.07/_DSC8298.ARW")
            .withCompressedFile(isCompressed)
            .withImportEvent(
                ImportEvent.builder()
                    .withAlbum("MyAlbum")
                    .withImportName("Mon import")
                    .withKeyWords(Arrays.asList("kw1", "kw2"))
                    .withScanFolder("Scan folder")
                    .build())
            .build();
    }

}
