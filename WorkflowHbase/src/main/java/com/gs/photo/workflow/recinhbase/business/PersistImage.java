package com.gs.photo.workflow.recinhbase.business;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.gs.photo.common.workflow.DateTimeHelper;
import com.gs.photo.workflow.recinhbase.dao.HbaseImageThumbnailDAO;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.builder.KeysBuilder.HbaseImageThumbnailKeyBuilder;
import com.workflow.model.events.WfEvent;
import com.workflow.model.events.WfEventRecorded;
import com.workflow.model.events.WfEventRecorded.RecordedEventType;
import com.workflow.model.events.WfEventStep;

@Service(PersistImage.MANAGED_CLASS)
public class PersistImage implements IPersistRecordsInDatabase<WfEvent, HbaseImageThumbnail> {

    static public final String       MANAGED_CLASS = "HbaseImageThumbnail";

    @Autowired
    protected HbaseImageThumbnailDAO hbaseImageThumbnailDAO;

    @Override
    public CompletableFuture<Collection<WfEvent>> persist(Collection<HbaseImageThumbnail> listOfRecords) {
        return PersistImage.alsoApply(
            CompletableFuture.supplyAsync(
                () -> this.recordCollectionBySimpleAppending(
                    listOfRecords.stream()
                        .filter(
                            t -> t.getThumbnail()
                                .containsKey(1))
                        .toList()),
                Executors.newVirtualThreadPerTaskExecutor()),
            CompletableFuture.supplyAsync(
                () -> this.recordCollectionByAppendingToTableFamily(
                    listOfRecords.stream()
                        .filter(
                            t -> !t.getThumbnail()
                                .containsKey(1))
                        .toList()),
                Executors.newVirtualThreadPerTaskExecutor())
                .thenApply(b -> a -> this.concat(a, b)));
    }

    private Collection<WfEvent> concat(Collection<WfEvent> a, Collection<WfEvent> b) {
        try {
            this.hbaseImageThumbnailDAO.flush();
            return Stream.concat(a.stream(), b.stream())
                .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static <T, R> CompletableFuture<R> alsoApply(CompletableFuture<T> future, CompletableFuture<Function<T, R>> f) {
        return f.thenCompose(future::thenApply);
    }

    private Collection<WfEvent> recordCollectionBySimpleAppending(Collection<HbaseImageThumbnail> listOfRecords) {
        try {
            listOfRecords = listOfRecords.stream()
                .map(t -> this.updateRegionSalt(t))
                .toList();
            this.hbaseImageThumbnailDAO.append(listOfRecords);
            return listOfRecords.stream()
                .map(t -> this.CreateEvent(t))
                .toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private Collection<WfEvent> recordCollectionByAppendingToTableFamily(
        Collection<HbaseImageThumbnail> listOfRecords
    ) {
        listOfRecords = listOfRecords.stream()
            .map(t -> this.updateRegionSalt(t))
            .toList();
        this.hbaseImageThumbnailDAO.append(listOfRecords, HbaseImageThumbnail.TABLE_FAMILY_THB);
        return listOfRecords.stream()
            .map(t -> this.CreateEvent(t))
            .toList();

    }

    private HbaseImageThumbnail updateRegionSalt(HbaseImageThumbnail hit) {
        OffsetDateTime ldt = DateTimeHelper.toLocalDateTime(hit.getCreationDate());
        hit.setRegionSalt((short) ldt.getDayOfMonth());
        return hit;
    }

    protected WfEvent CreateEvent(HbaseImageThumbnail x) {
        String hbdHashCode = HbaseImageThumbnailKeyBuilder.build(x);
        return WfEventRecorded.builder()
            .withImgId(x.getImageId())
            .withParentDataId(x.getDataId())
            .withDataId(hbdHashCode)
            .withStep(WfEventStep.WF_STEP_CREATED_FROM_STEP_RECORDED_IN_HBASE)
            .withImageCreationDate(x.getCreationDate())
            .withRecordedEventType(RecordedEventType.THUMB)
            .build();
    }

}
