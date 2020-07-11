package com.gs.photos.repositories.impl;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.gs.photo.workflow.exif.IExifService;
import com.gs.photo.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.dtos.ExifDTO;
import com.workflow.model.dtos.ImageExifDto;
import com.workflow.model.dtos.ImageKeyDto;

@Component
public class HbaseExifDataDAO extends GenericDAO<HbaseExifDataOfImages> {

    private static final byte[] FAMILY_EXV_BYTES  = "exv".getBytes();
    private static final byte[] FAMILY_IMD_BYTES  = "imd".getBytes();
    private static final byte[] FAMILY_SZ_BYTES   = "sz".getBytes();

    private static final byte[] COLUMN_EXV_BYTES  = "exv_bytes".getBytes();
    private static final byte[] COLUMN_EXV_INTS   = "exv_ints".getBytes();
    private static final byte[] COLUMN_EXV_SHORTS = "exv_shorts".getBytes();

    @Autowired
    protected IExifService      exifService;

    public ImageExifDto get(OffsetDateTime creationDate, String imageId, int version) throws IOException {
        List<ExifDTO> exifs = new ArrayList<>();
        ImageKeyDto imageKey = ImageKeyDto.builder()
            .withImageId(imageId)
            .withCreationDate(creationDate)
            .withVersion(version)
            .build();

        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = new Scan();
            scan.addColumn(HbaseExifDataDAO.FAMILY_EXV_BYTES, HbaseExifDataDAO.COLUMN_EXV_BYTES);
            scan.addColumn(HbaseExifDataDAO.FAMILY_EXV_BYTES, HbaseExifDataDAO.COLUMN_EXV_INTS);
            scan.addColumn(HbaseExifDataDAO.FAMILY_EXV_BYTES, HbaseExifDataDAO.COLUMN_EXV_SHORTS);

            HbaseExifDataOfImages hbt = HbaseExifDataOfImages.builder()
                .withImageId(imageId)
                .withExifTag((short) 0)
                .withExifPath(new short[] {})
                .build();

            byte[] keyStartValue = new byte[this.getHbaseDataInformation()
                .getKeyLength()];
            byte[] keyStopValue = new byte[this.getHbaseDataInformation()
                .getKeyLength()];

            this.hbaseDataInformation.buildKey(hbt, keyStartValue);
            scan.withStartRow(keyStartValue);

            hbt = HbaseExifDataOfImages.builder()
                .withImageId(imageId)
                .withExifTag((short) 65535)
                .withExifPath(new short[] {})
                .build();

            this.hbaseDataInformation.buildKey(hbt, keyStopValue);
            scan.withStopRow(keyStopValue, true);

            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {
                HbaseExifDataOfImages instance = new HbaseExifDataOfImages();
                this.hbaseDataInformation.build(instance, t);
                exifs.add(this.toExif(instance, imageKey));
            });
        }

        return ImageExifDto.builder()
            .withExifs(
                exifs.stream()
                    .distinct()
                    .collect(Collectors.toList()))
            .withImageOwner(
                ImageKeyDto.builder()
                    .withCreationDate(creationDate)
                    .withImageId(imageId)
                    .withVersion(version)
                    .build())
            .build();

    }

    private ExifDTO toExif(HbaseExifDataOfImages instance, ImageKeyDto imageKey) {

        ExifDTO.Builder builder = ExifDTO.builder();
        builder.withTagValue(instance.getExifTag())
            .withImageOwner(imageKey);

        final short[] exifPath = instance.getExifPath();
        return this.exifService
            .getExifDTOFrom(
                builder,
                exifPath[exifPath.length - 1],
                instance.getExifTag(),
                instance.getExifValueAsInt(),
                instance.getExifValueAsShort(),
                instance.getExifValueAsByte())
            .withPath(exifPath)
            .build();
    }

}
