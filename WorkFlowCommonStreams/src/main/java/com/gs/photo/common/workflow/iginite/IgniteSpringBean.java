/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gs.photo.common.workflow.iginite;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.DataRegionMetricsAdapter;
import org.apache.ignite.DataStorageMetrics;
import org.apache.ignite.DataStorageMetricsAdapter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteEncryption;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.tostring.GridToStringBuilder;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginNotFoundException;
import org.apache.ignite.spi.tracing.TracingConfigurationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Ignite Spring bean allows to bypass {@link Ignition} methods. In other words,
 * this bean class allows to inject new grid instance from Spring configuration
 * file directly without invoking static {@link Ignition} methods. This class
 * can be wired directly from Spring and can be referenced from within other
 * Spring beans. By virtue of implementing {@link DisposableBean} and
 * {@link SmartInitializingSingleton} interfaces, {@code IgniteSpringBean}
 * automatically starts and stops underlying grid instance.
 *
 * <p>
 * A note should be taken that Ignite instance is started after all other Spring
 * beans have been initialized and right before Spring context is refreshed.
 * That implies that it's not valid to reference IgniteSpringBean from any kind
 * of Spring bean init methods like {@link javax.annotation.PostConstruct}. If
 * it's required to reference IgniteSpringBean for other bean initialization
 * purposes, it should be done from a {@link ContextRefreshedEvent} listener
 * method declared in that bean.
 * </p>
 *
 * <p>
 * <h1 class="header">Spring Configuration Example</h1> Here is a typical
 * example of describing it in Spring file:
 *
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.apache.ignite.IgniteSpringBean"&gt;
 *     &lt;property name="configuration"&gt;
 *         &lt;bean id="grid.cfg" class="org.apache.ignite.configuration.IgniteConfiguration"&gt;
 *             &lt;property name="igniteInstanceName" value="mySpringGrid"/&gt;
 *         &lt;/bean&gt;
 *     &lt;/property&gt;
 * &lt;/bean&gt;
 * </pre>
 *
 * Or use default configuration:
 *
 * <pre name="code" class="xml">
 * &lt;bean id="mySpringBean" class="org.apache.ignite.IgniteSpringBean"/&gt;
 * </pre>
 *
 * <h1 class="header">Java Example</h1> Here is how you may access this bean
 * from code:
 *
 * <pre name="code" class="java">
 * AbstractApplicationContext ctx = new FileSystemXmlApplicationContext("/path/to/spring/file");
 *
 * // Register Spring hook to destroy bean automatically.
 * ctx.registerShutdownHook();
 *
 * Ignite ignite = (Ignite) ctx.getBean("mySpringBean");
 * </pre>
 * <p>
 */
public class IgniteSpringBean
    implements Ignite, DisposableBean, SmartInitializingSingleton, ApplicationContextAware, Externalizable {

    protected static final Logger LOGGER           = LoggerFactory.getLogger(IgniteSpringBean.class);
    /** */
    private static final long     serialVersionUID = 0L;

    /** */
    private Ignite                igniteApi;

    /** */
    private IgniteConfiguration   cfg;

    /** */
    private ApplicationContext    appCtx;

    /** {@inheritDoc} */
    @Override
    public IgniteConfiguration configuration() { return this.cfg; }

    /**
     * Gets the configuration of this Ignite instance.
     * <p>
     * This method is required for proper Spring integration and is the same as
     * {@link #configuration()}. See
     * https://issues.apache.org/jira/browse/IGNITE-1102 for details.
     * <p>
     * <b>NOTE:</b> <br>
     * SPIs obtains through this method should never be used directly. SPIs provide
     * internal view on the subsystem and is used internally by Ignite kernal. In
     * rare use cases when access to a specific implementation of this SPI is
     * required - an instance of this SPI can be obtained via this method to check
     * its configuration properties or call other non-SPI methods.
     *
     * @return Ignite configuration instance.
     * @see #configuration()
     */
    public IgniteConfiguration getConfiguration() { return this.cfg; }

    /**
     * Sets Ignite configuration.
     *
     * @param cfg
     *            Ignite configuration.
     */
    public void setConfiguration(IgniteConfiguration cfg) { this.cfg = cfg; }

    /**
     * Gets the spring application context this Ignite runs in.
     *
     * @return Application context this Ignite runs in.
     */
    public ApplicationContext getApplicationContext() throws BeansException { return this.appCtx; }

    /** {@inheritDoc} */
    @Override
    public void setApplicationContext(ApplicationContext ctx) throws BeansException { this.appCtx = ctx; }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws Exception {
        if (this.igniteApi != null) {
            // Do not cancel started tasks, wait for them.
            Ignition.stop(this.igniteApi.name(), false);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void afterSingletonsInstantiated() {
        if (this.cfg == null) {
            this.cfg = new IgniteConfiguration();
            IgniteSpringBean.LOGGER.info("Ignite configuration afterSingletonsInstantiated : deactivate exporters");
        }

        try {
            this.igniteApi = IgnitionEx.start(this.cfg);
        } catch (IgniteCheckedException e) {
            throw new IgniteException("Failed to start IgniteSpringBean", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public IgniteLogger log() {
        this.checkIgnite();

        return this.cfg.getGridLogger();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteProductVersion version() {
        this.checkIgnite();

        return this.igniteApi.version();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        this.checkIgnite();

        return this.igniteApi.compute();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteServices services() {
        this.checkIgnite();

        return this.igniteApi.services();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteMessaging message() {
        this.checkIgnite();

        return this.igniteApi.message();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteEvents events() {
        this.checkIgnite();

        return this.igniteApi.events();
    }

    /** {@inheritDoc} */
    @Override
    public ExecutorService executorService() {
        this.checkIgnite();

        return this.igniteApi.executorService();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCluster cluster() {
        this.checkIgnite();

        return this.igniteApi.cluster();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute(ClusterGroup grp) {
        this.checkIgnite();

        return this.igniteApi.compute(grp);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteMessaging message(ClusterGroup prj) {
        this.checkIgnite();

        return this.igniteApi.message(prj);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteEvents events(ClusterGroup grp) {
        this.checkIgnite();

        return this.igniteApi.events(grp);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteServices services(ClusterGroup grp) {
        this.checkIgnite();

        return this.igniteApi.services(grp);
    }

    /** {@inheritDoc} */
    @Override
    public ExecutorService executorService(ClusterGroup grp) {
        this.checkIgnite();

        return this.igniteApi.executorService(grp);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteScheduler scheduler() {
        this.checkIgnite();

        return this.igniteApi.scheduler();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        this.checkIgnite();

        return this.igniteApi.name();
    }

    /** {@inheritDoc} */
    @Override
    public void resetLostPartitions(Collection<String> cacheNames) {
        this.checkIgnite();

        this.igniteApi.resetLostPartitions(cacheNames);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRegionMetrics> dataRegionMetrics() {
        this.checkIgnite();

        return this.igniteApi.dataRegionMetrics();
    }

    /** {@inheritDoc} */

    @Override
    public DataRegionMetrics dataRegionMetrics(String memPlcName) {
        this.checkIgnite();

        return this.igniteApi.dataRegionMetrics(memPlcName);
    }

    /** {@inheritDoc} */
    @Override
    public DataStorageMetrics dataStorageMetrics() {
        this.checkIgnite();

        return this.igniteApi.dataStorageMetrics();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteEncryption encryption() {
        this.checkIgnite();

        return this.igniteApi.encryption();
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSnapshot snapshot() { return this.igniteApi.snapshot(); }

    /** {@inheritDoc} */
    @Override
    public TracingConfigurationManager tracingConfiguration() {
        this.checkIgnite();

        return this.igniteApi.tracingConfiguration();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<MemoryMetrics> memoryMetrics() {
        return DataRegionMetricsAdapter.collectionOf(this.dataRegionMetrics());
    }

    /** {@inheritDoc} */

    @Override
    public MemoryMetrics memoryMetrics(String memPlcName) {
        return DataRegionMetricsAdapter.valueOf(this.dataRegionMetrics(memPlcName));
    }

    /** {@inheritDoc} */
    @Override
    public PersistenceMetrics persistentStoreMetrics() {
        return DataStorageMetricsAdapter.valueOf(this.dataStorageMetrics());
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> cache(String name) {
        this.checkIgnite();

        return this.igniteApi.cache(name);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<String> cacheNames() {
        this.checkIgnite();

        return this.igniteApi.cacheNames();
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> createCache(CacheConfiguration<K, V> cacheCfg) {
        this.checkIgnite();

        return this.igniteApi.createCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(CacheConfiguration<K, V> cacheCfg) {
        this.checkIgnite();

        return this.igniteApi.getOrCreateCache(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> createCache(
        CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg
    ) {
        this.checkIgnite();

        return this.igniteApi.createCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<IgniteCache> createCaches(Collection<CacheConfiguration> cacheCfgs) {
        this.checkIgnite();

        return this.igniteApi.createCaches(cacheCfgs);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(
        CacheConfiguration<K, V> cacheCfg,
        NearCacheConfiguration<K, V> nearCfg
    ) {
        this.checkIgnite();

        return this.igniteApi.getOrCreateCache(cacheCfg, nearCfg);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> createNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        this.checkIgnite();

        return this.igniteApi.createNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> getOrCreateNearCache(String cacheName, NearCacheConfiguration<K, V> nearCfg) {
        this.checkIgnite();

        return this.igniteApi.getOrCreateNearCache(cacheName, nearCfg);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> getOrCreateCache(String cacheName) {
        this.checkIgnite();

        return this.igniteApi.getOrCreateCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<IgniteCache> getOrCreateCaches(Collection<CacheConfiguration> cacheCfgs) {
        this.checkIgnite();

        return this.igniteApi.getOrCreateCaches(cacheCfgs);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteCache<K, V> createCache(String cacheName) {
        this.checkIgnite();

        return this.igniteApi.createCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> void addCacheConfiguration(CacheConfiguration<K, V> cacheCfg) {
        this.checkIgnite();

        this.igniteApi.addCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override
    public void destroyCache(String cacheName) {
        this.checkIgnite();

        this.igniteApi.destroyCache(cacheName);
    }

    /** {@inheritDoc} */
    @Override
    public void destroyCaches(Collection<String> cacheNames) {
        this.checkIgnite();

        this.igniteApi.destroyCaches(cacheNames);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions transactions() {
        this.checkIgnite();

        return this.igniteApi.transactions();
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> IgniteDataStreamer<K, V> dataStreamer(String cacheName) {
        this.checkIgnite();

        return this.igniteApi.dataStreamer(cacheName);
    }

    /** {@inheritDoc} */
    @Override
    public <T extends IgnitePlugin> T plugin(String name) throws PluginNotFoundException {
        this.checkIgnite();

        return this.igniteApi.plugin(name);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteBinary binary() {
        this.checkIgnite();

        return this.igniteApi.binary();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IgniteException { this.igniteApi.close(); }

    /** {@inheritDoc} */

    @Override
    public IgniteAtomicSequence atomicSequence(String name, long initVal, boolean create) {
        this.checkIgnite();

        return this.igniteApi.atomicSequence(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteAtomicSequence atomicSequence(String name, AtomicConfiguration cfg, long initVal, boolean create)
        throws IgniteException {
        this.checkIgnite();

        return this.igniteApi.atomicSequence(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */

    @Override
    public IgniteAtomicLong atomicLong(String name, long initVal, boolean create) {
        this.checkIgnite();

        return this.igniteApi.atomicLong(name, initVal, create);
    }

    @Override
    public IgniteAtomicLong atomicLong(String name, AtomicConfiguration cfg, long initVal, boolean create)
        throws IgniteException {
        this.checkIgnite();

        return this.igniteApi.atomicLong(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */

    @Override
    public <T> IgniteAtomicReference<T> atomicReference(String name, T initVal, boolean create) {
        this.checkIgnite();

        return this.igniteApi.atomicReference(name, initVal, create);
    }

    /** {@inheritDoc} */
    @Override
    public <T> IgniteAtomicReference<T> atomicReference(String name, AtomicConfiguration cfg, T initVal, boolean create)
        throws IgniteException {
        this.checkIgnite();

        return this.igniteApi.atomicReference(name, cfg, initVal, create);
    }

    /** {@inheritDoc} */

    @Override
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(String name, T initVal, S initStamp, boolean create) {
        this.checkIgnite();

        return this.igniteApi.atomicStamped(name, initVal, initStamp, create);
    }

    @Override
    public <T, S> IgniteAtomicStamped<T, S> atomicStamped(
        String name,
        AtomicConfiguration cfg,
        T initVal,
        S initStamp,
        boolean create
    ) throws IgniteException {
        this.checkIgnite();

        return this.igniteApi.atomicStamped(name, cfg, initVal, initStamp, create);
    }

    /** {@inheritDoc} */

    @Override
    public IgniteCountDownLatch countDownLatch(String name, int cnt, boolean autoDel, boolean create) {
        this.checkIgnite();

        return this.igniteApi.countDownLatch(name, cnt, autoDel, create);
    }

    /** {@inheritDoc} */

    @Override
    public IgniteSemaphore semaphore(String name, int cnt, boolean failoverSafe, boolean create) {
        this.checkIgnite();

        return this.igniteApi.semaphore(name, cnt, failoverSafe, create);
    }

    /** {@inheritDoc} */

    @Override
    public IgniteLock reentrantLock(String name, boolean failoverSafe, boolean fair, boolean create) {
        this.checkIgnite();

        return this.igniteApi.reentrantLock(name, failoverSafe, fair, create);
    }

    /** {@inheritDoc} */

    @Override
    public <T> IgniteQueue<T> queue(String name, int cap, CollectionConfiguration cfg) {
        this.checkIgnite();

        return this.igniteApi.queue(name, cap, cfg);
    }

    /** {@inheritDoc} */

    @Override
    public <T> IgniteSet<T> set(String name, CollectionConfiguration cfg) {
        this.checkIgnite();

        return this.igniteApi.set(name, cfg);
    }

    /** {@inheritDoc} */
    @Override
    public <K> Affinity<K> affinity(String cacheName) { return this.igniteApi.affinity(cacheName); }

    /** {@inheritDoc} */
    @Override
    public boolean active() {
        this.checkIgnite();

        return this.igniteApi.active();
    }

    /** {@inheritDoc} */
    @Override
    public void active(boolean active) {
        this.checkIgnite();

        this.igniteApi.active(active);
    }

    /** {@inheritDoc} */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException { out.writeObject(this.igniteApi); }

    /** {@inheritDoc} */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.igniteApi = (Ignite) in.readObject();

        this.cfg = this.igniteApi.configuration();
    }

    /**
     * Checks if this bean is valid.
     *
     * @throws IllegalStateException
     *             If bean is not valid, i.e. Ignite has already been stopped or has
     *             not yet been started.
     */
    protected void checkIgnite() throws IllegalStateException {
        if (this.igniteApi == null) {
            throw new IllegalStateException("Ignite is in invalid state to perform this operation. "
                + "It either not started yet or has already being or have stopped.\n"
                + "Make sure that IgniteSpringBean is not referenced from any kind of Spring bean init methods "
                + "like @PostConstruct}.\n" + "[ignite=" + this.igniteApi + ", cfg=" + this.cfg + ']');
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() { return GridToStringBuilder.toString(IgniteSpringBean.class, this); }
}
