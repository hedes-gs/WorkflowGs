package com.gs.photo.workflow.recinhbase.business;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.instrumentation.TimedBean;
import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.workflow.recinhbase.dao.HbaseExifDataDAO;
import com.workflow.model.HbaseExifData;
import com.workflow.model.builder.KeysBuilder.HbaseExifDataKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;

import io.micrometer.core.annotation.Timed;

@Service(PersistExifRecord.MANAGED_CLASS)
@TimedBean
public class PersistExifRecord implements IPersistRecordsInDatabase<WfEvent, HbaseExifData> {
    static public final String MANAGED_CLASS = "HbaseExifData";
    @Autowired
    protected HbaseExifDataDAO HbaseExifDataDAO;

    @Override
    public CompletableFuture<Collection<WfEvent>> persist(Collection<HbaseExifData> listOfRecords) {
        return CompletableFuture
            .supplyAsync(() -> this.recordCollectionOf(listOfRecords), Executors.newVirtualThreadPerTaskExecutor());
    }

    @Timed
    private Collection<WfEvent> recordCollectionOf(Collection<HbaseExifData> listOfRecords) {
        try {
            listOfRecords = listOfRecords.stream()
                .map(t -> this.updateRegionSalt(t))
                .toList();
            this.HbaseExifDataDAO.put(listOfRecords);
            this.HbaseExifDataDAO.flush();
            return listOfRecords.stream()
                .map(t -> this.CreateEvent(t))
                .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private HbaseExifData updateRegionSalt(HbaseExifData t) {
        OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(t.getCreationDate());
        t.setRegionSalt((short) ldt.getDayOfMonth());
        return t;
    }

    private WfEvent CreateEvent(HbaseExifData hbd) {
        String hbdHashCode = HbaseExifDataKeyBuilder.build(hbd);
        return this.buildEvent(hbd.getImageId(), hbd.getDataId(), hbdHashCode);
    }

    private WfEvent buildEvent(String imageKey, String parentDataId, String dataId) {
        return WfEventRecorded.builder()
            .withDataId(dataId)
            .withParentDataId(parentDataId)
            .withImgId(imageKey)
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE)
            .withRecordedEventType(RecordedEventType.EXIF)
            .build();
    }

}
