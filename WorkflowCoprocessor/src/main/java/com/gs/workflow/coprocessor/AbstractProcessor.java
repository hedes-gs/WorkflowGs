package com.gs.workflow.coprocessor;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.gs.workflow.coprocessor.LockTableHelper.LockRequest;
import com.gs.workflow.coprocessor.LockTableHelper.LockResponse;
import com.gs.workflow.coprocessor.LockTableHelper._LockService;

public abstract class AbstractProcessor extends _LockService {

    protected static Logger   LOGGER               = LoggerFactory.getLogger(AbstractProcessor.class);
    private static final long INVALID_SEQ          = 0;
    final AtomicLong          writerSequenceNumber = new AtomicLong(AbstractProcessor.INVALID_SEQ);

    public AbstractProcessor() { super(); }

    protected Long getLock(Table metaDataPageTable) throws ServiceException, Throwable {
        Long lockNumber = new Random().nextLong();
        Map<byte[], Long> res = metaDataPageTable.coprocessorService(
            LockTableHelper._LockService.class,
            null,
            null,
            aggregate -> this.doLock(lockNumber, aggregate));
        AbstractPageProcessor.LOGGER
            .info("[COPROC][{}] getLock, lockNumber is {} - res is {}", this.getCoprocName(), lockNumber, res);
        return lockNumber;
    }

    protected Long doLock(Long lockNumber, LockTableHelper._LockService aggregate) throws IOException {
        AbstractPageProcessor.LOGGER
            .info("[COPROC][{}]doing lock {} for aggregate {}", this.getCoprocName(), lockNumber, aggregate);
        CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse>();
        LockTableHelper.LockRequest request = LockTableHelper.LockRequest.newBuilder()
            .setColumn("test")
            .setFamily("familytest")
            .setValue(lockNumber)
            .build();
        aggregate.lock(null, request, rpcCallback);
        LockTableHelper.LockResponse response = rpcCallback.get();
        return response.hasSum() ? response.getSum() : 0L;
    }

    protected Long releaseLock(Table metaDataPageTable, long lockNumber) throws ServiceException, Throwable {
        Map<byte[], Long> res = metaDataPageTable.coprocessorService(
            LockTableHelper._LockService.class,
            null,
            null,
            aggregate -> this.doReleaseLock(lockNumber, aggregate));
        return lockNumber;
    }

    private Long doReleaseLock(long lockNumber, LockTableHelper._LockService aggregate) throws IOException {
        CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<LockTableHelper.LockResponse>();
        LockTableHelper.LockRequest request = LockTableHelper.LockRequest.newBuilder()
            .setColumn("test")
            .setFamily("familytest")
            .setValue(lockNumber)
            .build();
        aggregate.releaseLock(null, request, rpcCallback);
        LockTableHelper.LockResponse response = rpcCallback.get();
        return response.hasSum() ? response.getSum() : 0L;
    }

    @Override
    public void lock(RpcController controller, LockRequest request, RpcCallback<LockResponse> done) {
        AbstractPageProcessor.LOGGER.info("[COPROC][{}]Trying to get lock...", this.getCoprocName());
        boolean isInterrupted = false;
        while (!this.writerSequenceNumber.compareAndSet(0, request.getValue())) {
            try {
                java.util.concurrent.TimeUnit.MILLISECONDS.sleep(100);

            } catch (InterruptedException e) {
                isInterrupted = true;
                break;
            }
        }
        if (!isInterrupted) {
            AbstractPageProcessor.LOGGER.info("[COPROC][{}]Region is locked...", this.getCoprocName());
            LockResponse response = LockResponse.newBuilder()
                .setSum(this.writerSequenceNumber.get())
                .build();
            done.run(response);
        }
    }

    protected abstract String getCoprocName();

}