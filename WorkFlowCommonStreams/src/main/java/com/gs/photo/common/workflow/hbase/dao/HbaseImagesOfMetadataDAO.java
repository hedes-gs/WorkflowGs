package com.gs.photo.common.workflow.hbase.dao;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gs.photo.common.workflow.hbase.HbaseDataInformation;
import com.gsphotos.worflow.hbasefilters.FilterRowFindNextRowWithinAMetaData;
import com.workflow.model.HbaseImagesOfMetadata;
import com.workflow.model.ModelConstants;

public abstract class HbaseImagesOfMetadataDAO<T1 extends HbaseImagesOfMetadata, T2> extends GenericDAO<T1>
    implements IHbaseImagesOfMetadataDAO<T1, T2> {
    private static Logger                  LOGGER                   = LoggerFactory
        .getLogger(HbaseImagesOfMetadataDAO.class);

    protected static final byte[]          FAMILY_SZ_AS_BYTES       = "sz".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_THB_AS_BYTES      = "thb".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_IMG_AS_BYTES      = "img".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_TECH_AS_BYTES     = "tech".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_ALBUMS_AS_BYTES   = "albums".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_KEYWORDS_AS_BYTES = "keywords".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_PERSONS_AS_BYTES  = "persons".getBytes(Charset.forName("UTF-8"));
    protected static final byte[]          FAMILY_META_AS_BYTES     = "meta".getBytes(Charset.forName("UTF-8"));

    protected Map<T2, LoadedPages<T1, T2>> imagesOfMetaData;

    protected static class PageDescription<T1 extends HbaseImagesOfMetadata, T2> {
        protected int      hbasePageNumber;
        protected List<T1> pageContent;

        public int getPageSize() { return this.pageContent.size(); }

        public PageDescription(int hbasePageNumber) {
            super();
            this.hbasePageNumber = hbasePageNumber;
            this.pageContent = new ArrayList<>();
        }

        public void add(T1 instance) { this.pageContent.add(instance); }

    }

    protected void deleteElementInAllMetadataFamilies(T1 him) {
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            Delete delete = new Delete(this.getKey(him));
            delete.addFamily(HbaseImagesOfMetadataDAO.FAMILY_PERSONS_AS_BYTES)
                .addFamily(HbaseImagesOfMetadataDAO.FAMILY_KEYWORDS_AS_BYTES)
                .addFamily(HbaseImagesOfMetadataDAO.FAMILY_ALBUMS_AS_BYTES)
                .addFamily(HbaseImagesOfMetadataDAO.FAMILY_META_AS_BYTES);
            table.delete(delete);
        } catch (IOException e) {
            HbaseImagesOfMetadataDAO.LOGGER.warn("Error when updating {} : {}", him, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }

    }

    protected static interface RowProvider { byte[] get(); }

    protected static interface FilterProvider<T2> { Filter get(); }

    protected static class LoadedPages<T1 extends HbaseImagesOfMetadata, T2> {

        @FunctionalInterface
        public static interface ProcessIfPresent<T1 extends HbaseImagesOfMetadata, T2> {
            public void process(PageDescription<T1, T2> page);
        }

        private static Logger                           LOGGER                   = LoggerFactory
            .getLogger(LoadedPages.class);

        protected Map<Integer, PageDescription<T1, T2>> currentDefaultLoadedPage = new TreeMap<>();
        protected HbaseDataInformation<T1>              hbaseDataInformation;
        protected RowProvider                           maxRowProvider;
        protected RowProvider                           minRowProvider;
        protected FilterProvider<T2>                    filterProvider;

        protected LoadedPages(
            HbaseDataInformation<T1> hbaseDataInformation,
            RowProvider maxRowProvider,
            RowProvider minRowProvider,
            FilterProvider<T2> filterProvider
        ) {
            super();
            this.hbaseDataInformation = hbaseDataInformation;
            this.maxRowProvider = maxRowProvider;
            this.minRowProvider = minRowProvider;
            this.filterProvider = filterProvider;
        }

        public static <T3 extends HbaseImagesOfMetadata, T2> LoadedPages<T3, T2> of(
            HbaseDataInformation<T3> hbaseDataInformation,
            RowProvider maxRowProvider,
            RowProvider minRowProvider,
            FilterProvider<T2> filterProvider
        ) {
            return new LoadedPages<>(hbaseDataInformation, minRowProvider, maxRowProvider, filterProvider);
        }

        private final int compareForSorting(T1 o1, T1 o2) {
            return Comparator.comparing(HbaseImagesOfMetadata::getCreationDate)
                .thenComparing(HbaseImagesOfMetadata::getImageId)
                .compare(o1, o2);
        }

        private T1 getLastRowOfPreviousPage(int currentPageNumber, List<RegionInfo> regions) {
            T1 lastOfPrevious = null;
            if (this.currentDefaultLoadedPage.containsKey(currentPageNumber - 1)) {
                PageDescription<T1, T2> pageDescription = this.currentDefaultLoadedPage.get(currentPageNumber - 1);
                if (pageDescription.getPageSize() > 0) {
                    lastOfPrevious = pageDescription.pageContent.get(0);
                }
            }
            return lastOfPrevious;
        }

        public int loadPages(Table table, int currentPageNumber, int pageSize, List<RegionInfo> regions) {
            final T1 lastOfPrevious = this.getLastRowOfPreviousPage(currentPageNumber, regions);
            PageDescription<T1, T2> pageDescription = this.currentDefaultLoadedPage.get(currentPageNumber);
            if ((pageDescription == null) || (pageDescription.getPageSize() < pageSize)) {
                pageDescription = this.createPageDescription(table, currentPageNumber, pageSize, lastOfPrevious);
                this.currentDefaultLoadedPage.put(currentPageNumber, pageDescription);
            }

            return pageDescription.getPageSize();
        }

        private PageDescription<T1, T2> createPageDescription(
            Table table,
            Integer currentPageNumber,
            int pageSize,
            T1 lastOfPrevious
        ) {
            try {
                PageDescription<T1, T2> retValue = new PageDescription<>(currentPageNumber);
                Scan scan = new Scan();
                scan.setReversed(true);
                scan.setCaching(10000);

                if (lastOfPrevious != null) {
                    GenericDAO.LOGGER.info("Last presious to [{}] - in page {} ", lastOfPrevious, currentPageNumber);
                    scan.withStartRow(GenericDAO.getKey(lastOfPrevious, this.hbaseDataInformation), false);
                    scan.withStopRow(this.minRowProvider.get());
                } else {
                    scan.withStartRow(this.minRowProvider.get());
                    scan.withStopRow(this.maxRowProvider.get());
                }
                scan.setLimit(pageSize);
                scan.setFilter(this.filterProvider.get());
                try (
                    ResultScanner rs = table.getScanner(scan)) {
                    LoadedPages.LOGGER.info(
                        "Trying to retrieve metadata of table {}, with key {}",
                        table.getName()
                            .getNameAsString());

                    rs.forEach((t) -> {
                        try {
                            T1 instance;
                            instance = this.hbaseDataInformation.newInstance();
                            this.hbaseDataInformation.build(instance, t);
                            retValue.add(instance);
                        } catch (
                            InstantiationException |
                            IllegalAccessException |
                            IllegalArgumentException |
                            InvocationTargetException |
                            NoSuchMethodException |
                            SecurityException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    retValue.pageContent = retValue.pageContent.stream()
                        .sorted((o1, o2) -> this.compareForSorting(o1, o2))
                        .collect(Collectors.toList());
                    return retValue;
                } finally {
                    GenericDAO.LOGGER.info(
                        "Current page {} with page size {}, found {} ",
                        currentPageNumber,
                        pageSize,
                        retValue.getPageSize());
                    retValue.pageContent.forEach(
                        (e) -> GenericDAO.LOGGER
                            .info("Current page {} with page size {}, found {} ", currentPageNumber, pageSize, e));
                }
            } catch (IOException e) {
                GenericDAO.LOGGER.warn("Error ", e);
                throw new RuntimeException(e);
            }
        }

        public boolean executeOnPage(int page, ProcessIfPresent<T1, T2> processIfPresent) {
            PageDescription<T1, T2> pageDesc = this.currentDefaultLoadedPage.get(page);
            boolean retValue = false;
            if (pageDesc != null) {
                processIfPresent.process(pageDesc);
                retValue = true;
            }
            return retValue;
        }

        public PageDescription<T1, T2> get(int currentPageNumber) {
            return this.currentDefaultLoadedPage.get(currentPageNumber);
        }
    }

    protected void clear(T2 key) { this.imagesOfMetaData.remove(key); }

    @Override
    public Optional<T1> getPrevious(T1 metaData) throws IOException {
        byte[] key = this.getKey(metaData);
        return this.getPreviousMetaDataOf(key)
            .stream()
            .sorted((t1, t2) -> this.compare(t1, t2))
            .findFirst();
    }

    @Override
    public Optional<T1> getNext(T1 metaData) throws IOException {
        byte[] key = this.getKey(metaData);
        return this.getPreviousMetaDataOf(key)
            .stream()
            .sorted((t1, t2) -> this.compare(t2, t1))
            .findFirst();
    }

    @Override
    public List<T1> getAllImagesOfMetadata(T2 key, int pageNumber, int pageSize) {
        try {
            LoadedPages<T1, T2> loadedPages = this.imagesOfMetaData
                .computeIfAbsent(key, (k) -> this.createLoadedPagesElement(key));
            List<RegionInfo> regions = null;
            long countAll = 0;
            try (
                Admin admin = this.connection.getAdmin()) {
                regions = admin.getRegions(
                    this.getHbaseDataInformation()
                        .getTable());
            }
            countAll = this.getNbOfElements(key);
            GenericDAO.LOGGER.info("Total nb of elements {} ", countAll);
            final int pageSize2 = (pageSize % regions.size()) == 0 ? pageSize / regions.size()
                : (int) Math.round((pageSize / regions.size()) + 1);
            if (pageNumber > 0) {
                List<T1> retValue = new ArrayList<>();
                try (
                    Table table = this.connection.getTable(
                        this.getHbaseDataInformation()
                            .getTable())) {
                    int totalLoadedElements = 0;
                    int currentPageNumber = 0;
                    int firstRow = pageSize * (pageNumber - 1);
                    int currentIndex = 0;
                    int initialIndex = 0;
                    int startPage = -1;
                    boolean startPageIsFound = false;

                    while ((totalLoadedElements < Math.min(countAll, pageSize * pageNumber))) {
                        final int nbOfElementsInPage = loadedPages
                            .loadPages(table, currentPageNumber, pageSize2, regions);
                        totalLoadedElements = totalLoadedElements + nbOfElementsInPage;
                        if (nbOfElementsInPage > 0) {
                            PageDescription<T1, T2> pageDesc = loadedPages.get(currentPageNumber);
                            if (((currentIndex + pageDesc.getPageSize()) >= firstRow) && !startPageIsFound) {
                                startPage = currentPageNumber;
                                initialIndex = firstRow - currentIndex;
                                startPageIsFound = true;
                                GenericDAO.LOGGER.info(
                                    "Loaded {}, found startPage {}, firstRow is {}",
                                    totalLoadedElements,
                                    startPage,
                                    firstRow);
                            }
                            currentIndex = currentIndex + pageDesc.getPageSize();
                            currentPageNumber++;
                            GenericDAO.LOGGER.info("Loaded {} where countAll is {}  ", totalLoadedElements, countAll);
                        } else {
                            GenericDAO.LOGGER.warn(
                                "Current page {} has no element, totalLoadedElements : {} , countAll is {} ",
                                currentPageNumber,
                                totalLoadedElements,
                                countAll);
                            break;
                        }
                    }
                    final int constStartPage = startPage;
                    final int constInitialIndex = initialIndex;
                    loadedPages.executeOnPage(constStartPage, (p) -> {
                        int page = constStartPage;
                        int lastIndex = Math.min(p.getPageSize(), constInitialIndex + (pageNumber * pageSize));
                        for (int k = constInitialIndex; k < lastIndex; k++) {
                            retValue.add(p.pageContent.get(k));
                        }
                        boolean pageIsProcessed = true;
                        do {
                            pageIsProcessed = loadedPages.executeOnPage(++page, (p1) -> {
                                for (int k = 0; (retValue.size() < pageSize) && (k < p1.getPageSize()); k++) {
                                    retValue.add(p1.pageContent.get(k));
                                }
                            });
                        } while ((retValue.size() < pageSize) && pageIsProcessed);
                    });

                } catch (IOException e) {
                    GenericDAO.LOGGER.warn("Error ", e);
                    throw new RuntimeException(e);
                }
                return retValue;
            } else {
                return Collections.EMPTY_LIST;
            }
        } catch (Throwable e) {
            GenericDAO.LOGGER.warn("Error ", e);
            throw new RuntimeException(e);
        }
    }

    protected LoadedPages<T1, T2> createLoadedPagesElement(T2 key) {
        return LoadedPages.of(
            this.getHbaseDataInformation(),
            () -> this.getMaxRowProvider(),
            () -> this.getMinRowProvider(),
            () -> this.getFilterFor(key));
    }

    protected List<T1> getPreviousMetaDataOf(byte[] keyOfMetadata) {
        List<T1> retValue = new ArrayList<>();
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {

            Scan scan = HbaseImagesOfMetadataDAO.createScanToGetAllColumns();
            scan.withStartRow(keyOfMetadata);
            scan.setFilter(
                new FilterList(
                    new FilterRowFindNextRowWithinAMetaData(keyOfMetadata,
                        this.getLengthOfMetaDataKey(),
                        this.getOffsetOfImageId(),
                        ModelConstants.FIXED_WIDTH_IMAGE_ID),
                    new PageFilter(1)));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                T1 instance;
                try {
                    instance = this.hbaseDataInformation.newInstance();
                    this.hbaseDataInformation.build(instance, t);
                    retValue.add(instance);
                } catch (
                    InstantiationException |
                    IllegalAccessException |
                    IllegalArgumentException |
                    InvocationTargetException |
                    NoSuchMethodException |
                    SecurityException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    protected List<T1> getNextMetaDataOf(byte[] keyOfMetadata) {
        List<T1> retValue = new ArrayList<>();
        try (
            Table table = AbstractDAO.getTable(this.connection, this.hbaseDataInformation.getTable())) {
            Scan scan = HbaseImagesOfMetadataDAO.createScanToGetAllColumns();
            scan.setReversed(true);
            scan.withStartRow(keyOfMetadata);
            scan.setFilter(
                new FilterList(
                    new FilterRowFindNextRowWithinAMetaData(keyOfMetadata,
                        this.getLengthOfMetaDataKey(),
                        this.getOffsetOfImageId(),
                        ModelConstants.FIXED_WIDTH_IMAGE_ID),
                    new PageFilter(1)));
            ResultScanner rs = table.getScanner(scan);
            rs.forEach((t) -> {

                T1 instance;
                try {
                    instance = this.hbaseDataInformation.newInstance();
                    this.getHbaseDataInformation()
                        .build(instance, t);
                    retValue.add(instance);
                } catch (
                    InstantiationException |
                    IllegalAccessException |
                    IllegalArgumentException |
                    InvocationTargetException |
                    NoSuchMethodException |
                    SecurityException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return retValue;
    }

    public long countNbOfPages() throws Throwable {
        try (
            AggregationClient ac = new AggregationClient(HBaseConfiguration.create())) {
            long retValue = ac
                .rowCount(this.hbaseDataInformation.getPageTable(), new LongColumnInterpreter(), new Scan());
            return retValue;
        }
    }

    protected static Scan createScanToGetAllColumns() {
        Scan scan = new Scan().addFamily(HbaseImagesOfMetadataDAO.FAMILY_IMG_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_THB_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_SZ_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_TECH_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_ALBUMS_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_KEYWORDS_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_PERSONS_AS_BYTES)
            .addFamily(HbaseImagesOfMetadataDAO.FAMILY_META_AS_BYTES);
        return scan;
    }

    @Override
    public void put(T1 hbaseData) throws IOException { this.put(hbaseData, this.getHbaseDataInformation()); }

    @PostConstruct
    protected void initHbaseImagesOfMetadataDAO() throws IOException { this.imagesOfMetaData = new HashMap<>(); }

    @Override
    public long countAll() throws IOException, Throwable { return 0; }

    @Override
    public long countAll(T1 metaData) throws IOException, Throwable { return 0; }

    @Override
    public long countAll(T2 key) throws IOException, Throwable { return 0; }

    /*
     * Abstract methods
     *
     */

    protected abstract int getOffsetOfImageId();

    protected abstract int getLengthOfMetaDataKey();

    protected abstract int compare(T1 t1, T1 t2);

    protected abstract byte[] getMinRowProvider();

    protected abstract byte[] getMaxRowProvider();

    protected abstract long getNbOfElements(T2 key);

    protected abstract Filter getFilterFor(T2 key);

}
