package com.gs.photo.workflow;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import javax.cache.CacheException;
import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorResult;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntry;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.QueryDetailMetrics;
import org.apache.ignite.cache.query.QueryMetrics;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.CacheMetricsMXBean;
import org.apache.ignite.transactions.TransactionException;

public class IgniteWrapper implements IgniteCache<String, byte[]> {

	protected IgniteCache<String, byte[]> igniteCache;
	protected Ignite                      beanIgnite;
	protected volatile Boolean            igniteCacheIsCreated = false;

	public IgniteWrapper(
			Ignite beanIgnite) {
		this.beanIgnite = beanIgnite;
	}

	private void createIgniteCache() {
		if (this.igniteCache == null) {
			synchronized (IgniteWrapper.class) {
				if (!this.igniteCacheIsCreated) {
					this.igniteCache = this.beanIgnite.getOrCreateCache(AbstractApplicationConfig.CACHE_NAME);
					this.igniteCacheIsCreated = true;
				}
			}
		}
	}

	@Override
	public void loadAll(Set<? extends String> keys, boolean replaceExistingValues,
			CompletionListener completionListener) {
		this.createIgniteCache();
		this.igniteCache.loadAll(keys,
				replaceExistingValues,
				completionListener);
	}

	@Override
	public String getName() {
		this.createIgniteCache();
		return this.igniteCache.getName();
	}

	@Override
	public CacheManager getCacheManager() {
		this.createIgniteCache();
		return this.igniteCache.getCacheManager();
	}

	@Override
	public boolean isClosed() {
		this.createIgniteCache();
		return this.igniteCache.isClosed();
	}

	@Override
	public <T> T unwrap(Class<T> clazz) {
		this.createIgniteCache();
		return this.igniteCache.unwrap(clazz);
	}

	@Override
	public void registerCacheEntryListener(
			CacheEntryListenerConfiguration<String, byte[]> cacheEntryListenerConfiguration) {
		this.createIgniteCache();
		this.igniteCache.registerCacheEntryListener(cacheEntryListenerConfiguration);

	}

	@Override
	public void deregisterCacheEntryListener(
			CacheEntryListenerConfiguration<String, byte[]> cacheEntryListenerConfiguration) {
		this.createIgniteCache();
		this.igniteCache.deregisterCacheEntryListener(cacheEntryListenerConfiguration);

	}

	@Override
	public Iterator<Entry<String, byte[]>> iterator() {
		this.createIgniteCache();
		return this.igniteCache.iterator();
	}

	@Override
	public boolean isAsync() {
		this.createIgniteCache();
		return this.igniteCache.isAsync();
	}

	@Override
	public <R> IgniteFuture<R> future() {
		this.createIgniteCache();
		return this.igniteCache.future();
	}

	@Override
	public IgniteCache<String, byte[]> withAsync() {
		this.createIgniteCache();
		return this.igniteCache.withAsync();
	}

	@Override
	public <C extends Configuration<String, byte[]>> C getConfiguration(Class<C> clazz) {
		this.createIgniteCache();
		return this.igniteCache.getConfiguration(clazz);
	}

	@Override
	public IgniteCache<String, byte[]> withExpiryPolicy(ExpiryPolicy plc) {
		this.createIgniteCache();
		return this.igniteCache.withExpiryPolicy(plc);
	}

	@Override
	public IgniteCache<String, byte[]> withSkipStore() {
		this.createIgniteCache();
		return this.igniteCache.withSkipStore();
	}

	@Override
	public IgniteCache<String, byte[]> withNoRetries() {
		this.createIgniteCache();
		return this.igniteCache.withNoRetries();
	}

	@Override
	public IgniteCache<String, byte[]> withPartitionRecover() {
		this.createIgniteCache();
		return this.igniteCache.withPartitionRecover();
	}

	@Override
	public <K1, V1> IgniteCache<K1, V1> withKeepBinary() {
		this.createIgniteCache();
		return this.igniteCache.withKeepBinary();
	}

	@Override
	public <K1, V1> IgniteCache<K1, V1> withAllowAtomicOpsInTx() {
		this.createIgniteCache();
		return this.igniteCache.withAllowAtomicOpsInTx();
	}

	@Override
	public void loadCache(IgniteBiPredicate<String, byte[]> p, Object... args) throws CacheException {
		this.createIgniteCache();
		this.igniteCache.loadCache(p,
				args);
	}

	@Override
	public IgniteFuture<Void> loadCacheAsync(IgniteBiPredicate<String, byte[]> p, Object... args)
			throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.loadCacheAsync(p,
				args);
	}

	@Override
	public void localLoadCache(IgniteBiPredicate<String, byte[]> p, Object... args) throws CacheException {
		this.createIgniteCache();
		this.igniteCache.localLoadCache(p,
				args);
	}

	@Override
	public IgniteFuture<Void> localLoadCacheAsync(IgniteBiPredicate<String, byte[]> p, Object... args)
			throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.localLoadCacheAsync(p,
				args);
	}

	@Override
	public byte[] getAndPutIfAbsent(String key, byte[] val) throws CacheException, TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndPutIfAbsent(key,
				val);
	}

	@Override
	public IgniteFuture<byte[]> getAndPutIfAbsentAsync(String key, byte[] val)
			throws CacheException, TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndPutIfAbsentAsync(key,
				val);
	}

	@Override
	public Lock lock(String key) {
		this.createIgniteCache();
		return this.igniteCache.lock(key);
	}

	@Override
	public Lock lockAll(Collection<? extends String> keys) {
		this.createIgniteCache();
		return this.igniteCache.lockAll(keys);
	}

	@Override
	public boolean isLocalLocked(String key, boolean byCurrThread) {
		this.createIgniteCache();
		return this.igniteCache.isLocalLocked(key,
				byCurrThread);
	}

	@Override
	public <R> QueryCursor<R> query(Query<R> qry) {
		this.createIgniteCache();
		return this.igniteCache.query(qry);
	}

	@Override
	public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
		this.createIgniteCache();
		return this.igniteCache.query(qry);
	}

	@Override
	public <T, R> QueryCursor<R> query(Query<T> qry, IgniteClosure<T, R> transformer) {
		this.createIgniteCache();
		return this.igniteCache.query(qry,
				transformer);
	}

	@Override
	public Iterable<Entry<String, byte[]>> localEntries(CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.localEntries(peekModes);
	}

	@Override
	public QueryMetrics queryMetrics() {
		this.createIgniteCache();
		return this.igniteCache.queryMetrics();
	}

	@Override
	public void resetQueryMetrics() {
		this.createIgniteCache();
		this.igniteCache.resetQueryMetrics();
	}

	@Override
	public Collection<? extends QueryDetailMetrics> queryDetailMetrics() {
		this.createIgniteCache();
		return this.igniteCache.queryDetailMetrics();
	}

	@Override
	public void resetQueryDetailMetrics() {
		this.createIgniteCache();
		this.igniteCache.resetQueryDetailMetrics();
	}

	@Override
	public void localEvict(Collection<? extends String> keys) {
		this.createIgniteCache();
		this.igniteCache.localEvict(keys);
	}

	@Override
	public byte[] localPeek(String key, CachePeekMode... peekModes) {
		this.createIgniteCache();
		return this.igniteCache.localPeek(key,
				peekModes);
	}

	@Override
	public int size(CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.size(peekModes);
	}

	@Override
	public IgniteFuture<Integer> sizeAsync(CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.sizeAsync(peekModes);
	}

	@Override
	public long sizeLong(CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.sizeLong(peekModes);
	}

	@Override
	public IgniteFuture<Long> sizeLongAsync(CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.sizeLongAsync(peekModes);
	}

	@Override
	public long sizeLong(int partition, CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.sizeLong(partition,
				peekModes);
	}

	@Override
	public IgniteFuture<Long> sizeLongAsync(int partition, CachePeekMode... peekModes) throws CacheException {
		this.createIgniteCache();
		return this.igniteCache.sizeLongAsync(partition,
				peekModes);
	}

	@Override
	public int localSize(CachePeekMode... peekModes) {
		this.createIgniteCache();
		return this.igniteCache.localSize(peekModes);
	}

	@Override
	public long localSizeLong(CachePeekMode... peekModes) {
		this.createIgniteCache();
		return this.igniteCache.localSizeLong(peekModes);
	}

	@Override
	public long localSizeLong(int partition, CachePeekMode... peekModes) {
		this.createIgniteCache();
		return this.igniteCache.localSizeLong(partition,
				peekModes);
	}

	@Override
	public <T> Map<String, EntryProcessorResult<T>> invokeAll(
			Map<? extends String, ? extends EntryProcessor<String, byte[], T>> map, Object... args)
			throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAll(map,
				args);
	}

	@Override
	public <T> IgniteFuture<Map<String, EntryProcessorResult<T>>> invokeAllAsync(
			Map<? extends String, ? extends EntryProcessor<String, byte[], T>> map, Object... args)
			throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAllAsync(map,
				args);
	}

	@Override
	public byte[] get(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.get(key);
	}

	@Override
	public IgniteFuture<byte[]> getAsync(String key) {
		this.createIgniteCache();
		return this.igniteCache.getAsync(key);
	}

	@Override
	public CacheEntry<String, byte[]> getEntry(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getEntry(key);
	}

	@Override
	public IgniteFuture<CacheEntry<String, byte[]>> getEntryAsync(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getEntryAsync(key);
	}

	@Override
	public Map<String, byte[]> getAll(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAll(keys);
	}

	@Override
	public IgniteFuture<Map<String, byte[]>> getAllAsync(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAllAsync(keys);
	}

	@Override
	public Collection<CacheEntry<String, byte[]>> getEntries(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getEntries(keys);
	}

	@Override
	public IgniteFuture<Collection<CacheEntry<String, byte[]>>> getEntriesAsync(Set<? extends String> keys)
			throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getEntriesAsync(keys);
	}

	@Override
	public Map<String, byte[]> getAllOutTx(Set<? extends String> keys) {
		this.createIgniteCache();
		return this.igniteCache.getAllOutTx(keys);
	}

	@Override
	public IgniteFuture<Map<String, byte[]>> getAllOutTxAsync(Set<? extends String> keys) {
		this.createIgniteCache();
		return this.igniteCache.getAllOutTxAsync(keys);
	}

	@Override
	public boolean containsKey(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.containsKey(key);
	}

	@Override
	public IgniteFuture<Boolean> containsKeyAsync(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.containsKeyAsync(key);
	}

	@Override
	public boolean containsKeys(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.containsKeys(keys);
	}

	@Override
	public IgniteFuture<Boolean> containsKeysAsync(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.containsKeysAsync(keys);
	}

	@Override
	public void put(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		this.igniteCache.put(key,
				val);
	}

	@Override
	public IgniteFuture<Void> putAsync(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.putAsync(key,
				val);
	}

	@Override
	public byte[] getAndPut(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndPut(key,
				val);
	}

	@Override
	public IgniteFuture<byte[]> getAndPutAsync(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndPutAsync(key,
				val);

	}

	@Override
	public void putAll(Map<? extends String, ? extends byte[]> map) throws TransactionException {
		this.createIgniteCache();
		this.igniteCache.putAll(map);

	}

	@Override
	public IgniteFuture<Void> putAllAsync(Map<? extends String, ? extends byte[]> map) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.putAllAsync(map);
	}

	@Override
	public boolean putIfAbsent(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.putIfAbsent(key,
				val);
	}

	@Override
	public IgniteFuture<Boolean> putIfAbsentAsync(String key, byte[] val) {
		this.createIgniteCache();
		return this.igniteCache.putIfAbsentAsync(key,
				val);
	}

	@Override
	public boolean remove(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.remove(key);
	}

	@Override
	public IgniteFuture<Boolean> removeAsync(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.removeAsync(key);
	}

	@Override
	public boolean remove(String key, byte[] oldVal) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.remove(key,
				oldVal);
	}

	@Override
	public IgniteFuture<Boolean> removeAsync(String key, byte[] oldVal) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.removeAsync(key,
				oldVal);
	}

	@Override
	public byte[] getAndRemove(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndRemove(key);
	}

	@Override
	public IgniteFuture<byte[]> getAndRemoveAsync(String key) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndRemoveAsync(key);
	}

	@Override
	public boolean replace(String key, byte[] oldVal, byte[] newVal) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.replace(key,
				oldVal,
				newVal);
	}

	@Override
	public IgniteFuture<Boolean> replaceAsync(String key, byte[] oldVal, byte[] newVal) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.replaceAsync(key,
				oldVal,
				newVal);
	}

	@Override
	public boolean replace(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.replace(key,
				val);
	}

	@Override
	public IgniteFuture<Boolean> replaceAsync(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.replaceAsync(key,
				val);
	}

	@Override
	public byte[] getAndReplace(String key, byte[] val) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.getAndReplace(key,
				val);
	}

	@Override
	public IgniteFuture<byte[]> getAndReplaceAsync(String key, byte[] val) {
		this.createIgniteCache();
		return this.igniteCache.getAndReplaceAsync(key,
				val);
	}

	@Override
	public void removeAll(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		this.igniteCache.removeAll(keys);
	}

	@Override
	public IgniteFuture<Void> removeAllAsync(Set<? extends String> keys) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.removeAllAsync(keys);
	}

	@Override
	public void removeAll() {
		this.createIgniteCache();
		this.igniteCache.removeAll();
	}

	@Override
	public IgniteFuture<Void> removeAllAsync() {
		this.createIgniteCache();
		return this.igniteCache.removeAllAsync();
	}

	@Override
	public void clear() {
		this.createIgniteCache();
		this.igniteCache.clear();
	}

	@Override
	public IgniteFuture<Void> clearAsync() {
		this.createIgniteCache();
		return this.igniteCache.clearAsync();
	}

	@Override
	public void clear(String key) {
		this.createIgniteCache();
		this.igniteCache.clear(key);
	}

	@Override
	public IgniteFuture<Void> clearAsync(String key) {
		this.createIgniteCache();
		return this.igniteCache.clearAsync(key);
	}

	@Override
	public void clearAll(Set<? extends String> keys) {
		this.createIgniteCache();
		this.igniteCache.clearAll(keys);
	}

	@Override
	public IgniteFuture<Void> clearAllAsync(Set<? extends String> keys) {
		this.createIgniteCache();
		return this.igniteCache.clearAllAsync(keys);
	}

	@Override
	public void localClear(String key) {
		this.createIgniteCache();
		this.igniteCache.localClear(key);
	}

	@Override
	public void localClearAll(Set<? extends String> keys) {
		this.createIgniteCache();
		this.igniteCache.localClearAll(keys);
	}

	@Override
	public <T> T invoke(String key, EntryProcessor<String, byte[], T> entryProcessor, Object... arguments)
			throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invoke(key,
				entryProcessor,
				arguments);
	}

	@Override
	public <T> IgniteFuture<T> invokeAsync(String key, EntryProcessor<String, byte[], T> entryProcessor,
			Object... arguments) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAsync(key,
				entryProcessor,
				arguments);
	}

	@Override
	public <T> T invoke(String key, CacheEntryProcessor<String, byte[], T> entryProcessor, Object... arguments)
			throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invoke(key,
				entryProcessor,
				arguments);
	}

	@Override
	public <T> IgniteFuture<T> invokeAsync(String key, CacheEntryProcessor<String, byte[], T> entryProcessor,
			Object... arguments) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAsync(key,
				entryProcessor,
				arguments);
	}

	@Override
	public <T> Map<String, EntryProcessorResult<T>> invokeAll(Set<? extends String> keys,
			EntryProcessor<String, byte[], T> entryProcessor, Object... args) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAll(keys,
				entryProcessor,
				args);
	}

	@Override
	public <T> IgniteFuture<Map<String, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends String> keys,
			EntryProcessor<String, byte[], T> entryProcessor, Object... args) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAllAsync(keys,
				entryProcessor,
				args);
	}

	@Override
	public <T> Map<String, EntryProcessorResult<T>> invokeAll(Set<? extends String> keys,
			CacheEntryProcessor<String, byte[], T> entryProcessor, Object... args) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAll(keys,
				entryProcessor,
				args);
	}

	@Override
	public <T> IgniteFuture<Map<String, EntryProcessorResult<T>>> invokeAllAsync(Set<? extends String> keys,
			CacheEntryProcessor<String, byte[], T> entryProcessor, Object... args) throws TransactionException {
		this.createIgniteCache();
		return this.igniteCache.invokeAllAsync(keys,
				entryProcessor,
				args);
	}

	@Override
	public void close() {
		this.createIgniteCache();
		this.igniteCache.close();
	}

	@Override
	public void destroy() {
		this.createIgniteCache();
		this.igniteCache.destroy();
	}

	@Override
	public IgniteFuture<Boolean> rebalance() {
		this.createIgniteCache();
		return this.igniteCache.rebalance();
	}

	@Override
	public IgniteFuture<?> indexReadyFuture() {
		this.createIgniteCache();
		return this.igniteCache.indexReadyFuture();
	}

	@Override
	public CacheMetrics metrics() {
		this.createIgniteCache();
		return this.igniteCache.metrics();
	}

	@Override
	public CacheMetrics metrics(ClusterGroup grp) {
		this.createIgniteCache();
		return this.igniteCache.metrics(grp);
	}

	@Override
	public CacheMetrics localMetrics() {
		this.createIgniteCache();
		return this.igniteCache.localMetrics();
	}

	@Override
	public CacheMetricsMXBean mxBean() {
		this.createIgniteCache();
		return this.igniteCache.mxBean();
	}

	@Override
	public CacheMetricsMXBean localMxBean() {
		this.createIgniteCache();
		return this.igniteCache.localMxBean();
	}

	@Override
	public Collection<Integer> lostPartitions() {
		this.createIgniteCache();
		return this.igniteCache.lostPartitions();
	}

	@Override
	public void enableStatistics(boolean enabled) {
		this.createIgniteCache();
		this.igniteCache.enableStatistics(enabled);
	}

	@Override
	public void clearStatistics() {
		this.createIgniteCache();
		this.igniteCache.clearStatistics();
	}

}
