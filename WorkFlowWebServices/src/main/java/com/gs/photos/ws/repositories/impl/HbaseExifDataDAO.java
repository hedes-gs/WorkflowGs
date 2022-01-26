package com.gs.photos.ws.repositories.impl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteRequest.DeleteType;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteResponse;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos.BulkDeleteService;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.gs.photo.common.workflow.exif.IExifService;
import com.gs.photo.common.workflow.hbase.dao.GenericDAO;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.ModelConstants;
import com.workflow.model.dtos.ExifDTO;
import com.workflow.model.dtos.ImageExifDto;
import com.workflow.model.dtos.ImageKeyDto;

@Component
public class HbaseExifDataDAO extends GenericDAO<HbaseExifDataOfImages> {

    protected static final Logger LOGGER            = LoggerFactory.getLogger(HbaseExifDataDAO.class);
    private static final byte[]   FAMILY_EXV_BYTES  = "exv".getBytes();
    private static final byte[]   FAMILY_IMD_BYTES  = "imd".getBytes();
    private static final byte[]   FAMILY_SZ_BYTES   = "sz".getBytes();

    private static final byte[]   COLUMN_EXV_BYTES  = "exv_bytes".getBytes();
    private static final byte[]   COLUMN_EXV_INTS   = "exv_ints".getBytes();
    private static final byte[]   COLUMN_EXV_SHORTS = "exv_shorts".getBytes();

    @Autowired
    protected IExifService        exifService;

    public ImageExifDto get(short salt, OffsetDateTime creationDate, String imageId, int version) throws IOException {
        List<ExifDTO> exifs = new ArrayList<>();
        ImageKeyDto imageKey = ImageKeyDto.builder()
            .withSalt(salt)
            .withImageId(imageId)
            .withCreationDate(creationDate)
            .withVersion(version)
            .build();

        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = this.createExifScan(salt, imageId);
            scan.addFamily(HbaseExifDataDAO.FAMILY_EXV_BYTES);

            try (
                ResultScanner rs = table.getScanner(scan)) {
                rs.forEach((t) -> {
                    HbaseExifDataOfImages instance = new HbaseExifDataOfImages();
                    this.hbaseDataInformation.build(instance, t);
                    this.toExif(instance, imageKey)
                        .ifPresent((a) -> exifs.add(a));

                });
            }
        }
        HbaseExifDataDAO.LOGGER.info("End of Retreive exif for salt {} - {} ", salt, imageId);

        return ImageExifDto.builder()
            .withExifs(
                exifs.stream()
                    .distinct()
                    .collect(Collectors.toList()))
            .withImageOwner(
                ImageKeyDto.builder()
                    .withSalt(salt)
                    .withCreationDate(creationDate)
                    .withImageId(imageId)
                    .withVersion(version)
                    .build())
            .build();

    }

    private Scan createExifScan(short salt, String imageId) throws UnsupportedEncodingException {
        Scan scan = new Scan();
        final byte[] imageIdAsbytes = imageId.getBytes("UTF-8");
        byte[] prefixFilter = new byte[2 + ModelConstants.FIXED_WIDTH_IMAGE_ID];
        HbaseExifDataDAO.LOGGER.info("Retreive exif for salt {} - {} ", salt, imageId);
        Bytes.putAsShort(prefixFilter, 0, salt);
        System.arraycopy(imageIdAsbytes, 0, prefixFilter, 2, imageIdAsbytes.length);
        scan.setFilter(new PrefixFilter(prefixFilter));
        byte[] stopRow = new byte[2 + ModelConstants.FIXED_WIDTH_IMAGE_ID + ModelConstants.FIXED_WIDTH_EXIF_TAG];
        Bytes.putAsShort(prefixFilter, 0, salt);
        System.arraycopy(imageIdAsbytes, 0, prefixFilter, 2, imageIdAsbytes.length);
        Arrays.fill(stopRow, (byte) 255);
        Bytes.putAsShort(stopRow, 0, salt);
        System.arraycopy(imageIdAsbytes, 0, stopRow, 2, imageIdAsbytes.length);
        scan.withStartRow(prefixFilter, true)
            .withStopRow(stopRow);
        return scan;
    }

    private Optional<ExifDTO> toExif(HbaseExifDataOfImages instance, ImageKeyDto imageKey) {

        ExifDTO.Builder builder = ExifDTO.builder();
        builder.withTagValue(instance.getExifTag())
            .withImageOwner(imageKey);

        final short[] exifPath = instance.getExifPath();
        try {
            return Optional.of(
                this.exifService
                    .getExifDTOFrom(
                        builder,
                        exifPath[exifPath.length - 1],
                        instance.getExifTag(),
                        instance.getExifValueAsInt(),
                        instance.getExifValueAsShort(),
                        instance.getExifValueAsByte())
                    .withPath(exifPath)
                    .build());
        } catch (Exception e) {
            HbaseExifDataDAO.LOGGER.warn("Error when retrieving exif {}", e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public void delete(HbaseExifDataOfImages hbaseData, String family, String column) { // TODO Auto-generated method
                                                                                        // stub
    }

    public void delete(short salt, String imageId) throws ServiceException, Throwable {
        try (
            Table table = this.connection.getTable(
                this.getHbaseDataInformation()
                    .getTable())) {
            Scan scan = this.createExifScan(salt, imageId);
            int noOfDeletedRows = 0;
            // set scan properties(rowkey range, filters, timerange etc).
            Batch.Call<BulkDeleteService, BulkDeleteResponse> callable = new Batch.Call<BulkDeleteService, BulkDeleteResponse>() {
                RpcController                                               controller  = new ServerRpcController();
                CoprocessorRpcUtils.BlockingRpcCallback<BulkDeleteResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();

                @Override
                public BulkDeleteResponse call(BulkDeleteService service) throws IOException {
                    BulkDeleteProtos.BulkDeleteRequest.Builder builder = BulkDeleteRequest.newBuilder();
                    builder.setScan(ProtobufUtil.toScan(scan));
                    builder.setDeleteType(DeleteType.ROW);
                    builder.setRowBatchSize(150);
                    service.delete(this.controller, builder.build(), this.rpcCallback);
                    return this.rpcCallback.get();
                }
            };
            Map<byte[], BulkDeleteResponse> result = table
                .coprocessorService(BulkDeleteService.class, scan.getStartRow(), scan.getStopRow(), callable);
            for (BulkDeleteResponse response : result.values()) {
                noOfDeletedRows += response.getRowsDeleted();
            }
            HbaseExifDataDAO.LOGGER.info(
                "Delete {} rows from table {} with salt = {} and imageId={}",
                noOfDeletedRows,
                table,
                salt,
                imageId);
        }
    }
}