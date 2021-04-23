package com.gs.photos.ws;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.DateTimeException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.boot.autoconfigure.web.format.WebConversionService;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.format.datetime.standard.DateTimeFormatterRegistrar;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.web.reactive.config.WebFluxConfigurer;

import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.datatype.jsr310.deser.JSR310DateTimeDeserializerBase;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.datatype.jsr310.ser.OffsetDateTimeSerializer;
import com.gs.photo.common.workflow.AbstractApplicationConfig;
import com.gs.photo.common.workflow.DateTimeHelper;
import com.workflow.model.dtos.ImageDto;
import com.workflow.model.dtos.ImageKeyDto;

@Configuration(proxyBeanMethods = false)
@EnableCaching
public class ApplicationConfig extends AbstractApplicationConfig {

    private static final String HBASE_MASTER_KERBEROS_PRINCIPAL       = "hbase.master.kerberos.principal";
    private static final String HBASE_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
    private static final String HBASE_RPC_PROTECTION                  = "hbase.rpc.protection";
    private static final String HBASE_SECURITY_AUTHENTICATION         = "hbase.security.authentication";
    private static final String HADOOP_SECURITY_AUTHENTICATION        = "hadoop.security.authentication";
    private static final String CONSUMER_IMAGE                        = "consumer-image";
    private static final String CONSUMER_EXIF                         = "consumer-exif";
    private static final String CONSUMER_EXIF_DATA_OF_IMAGES          = "consumer-exif-data-of-images";

    private static Logger       LOGGER                                = LoggerFactory
        .getLogger(ApplicationConfig.class);

    @Bean
    protected org.apache.hadoop.conf.Configuration hbaseConfiguration(
        @Value("${zookeeper.hosts}") String zookeeperHosts,
        @Value("${zookeeper.port}") int zookeeperPort
    ) {

        org.apache.hadoop.conf.Configuration hBaseConfig = HBaseConfiguration.create();
        hBaseConfig.setInt("timeout", 120000);
        hBaseConfig.set(HConstants.ZOOKEEPER_QUORUM, zookeeperHosts);
        hBaseConfig.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, zookeeperPort);
        hBaseConfig.set(ApplicationConfig.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(ApplicationConfig.HBASE_SECURITY_AUTHENTICATION, "kerberos");
        hBaseConfig.set(HConstants.CLUSTER_DISTRIBUTED, "true");
        hBaseConfig.set(ApplicationConfig.HBASE_RPC_PROTECTION, "authentication");
        hBaseConfig.set(ApplicationConfig.HBASE_REGIONSERVER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");
        hBaseConfig.set(ApplicationConfig.HBASE_MASTER_KERBEROS_PRINCIPAL, "hbase/_HOST@GS.COM");

        return hBaseConfig;
    }

    @Bean
    public Connection hbaseConnection(
        @Autowired org.apache.hadoop.conf.Configuration hbaseConfiguration,
        @Value("${application.gs.principal}") String principal,
        @Value("${application.gs.keytab}") String keytab
    ) {

        ApplicationConfig.LOGGER.info("creating the hbase connection, {} ", WebFluxConfigurer.class);
        UserGroupInformation.setConfiguration(hbaseConfiguration);
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        } catch (IOException e) {
            ApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            throw new RuntimeException(e);
        }
        PrivilegedAction<Connection> action = () -> {
            try {
                return ConnectionFactory.createConnection(hbaseConfiguration);
            } catch (IOException e) {
                ApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            }
            return null;
        };
        try {
            return UserGroupInformation.getCurrentUser()
                .doAs(action);
        } catch (IOException e) {
            ApplicationConfig.LOGGER.error("Error when creating hbaseConnection", e);
            throw new RuntimeException(e);
        }
    }

    @Bean
    public CacheManager cacheManager(
        @Value("${cache.images.size}") int cacheImagesSize,
        @Value("${cache.images.expireinseconds}") int cacheImagesExpire,
        @Value("${cache.images.name}") String cacheImagesName,
        @Value("${cache.jpegimages.size}") int cacheJpegImagesSize,
        @Value("${cache.jpegimages.expireinseconds}") int cacheJpegImagesExpire,
        @Value("${cache.jpegimages.name}") String cacheJpegImagesName,
        @Value("${cache.jpegimages.version.name}") String cacheJpegImagesVersionName,
        @Value("${cache.exif.size}") int cacheExifSize,
        @Value("${cache.exif.name}") String cacheExifName,
        @Value("${cache.exif.expireinseconds}") int cacheExifExpire
    ) {
        return new SpringCache2kCacheManager().defaultSetup(b -> b.entryCapacity(500))
            .addCaches(
                b -> b.name(cacheImagesName)
                    .expireAfterWrite(cacheImagesExpire, TimeUnit.SECONDS)
                    .entryCapacity(cacheImagesSize)
                    .keyType(ImageKeyDto.class)
                    .valueType(ImageDto.class),
                /*
                 * b -> b.name(cacheJpegImagesName) .expireAfterWrite(cacheJpegImagesExpire,
                 * TimeUnit.SECONDS) .entryCapacity(cacheJpegImagesSize) .keyType(String.class)
                 * .valueType(byte[].class),
                 */
                b -> b.name(cacheJpegImagesVersionName)
                    .expireAfterWrite(cacheJpegImagesExpire, TimeUnit.SECONDS)
                    .entryCapacity(cacheJpegImagesSize)
                    .keyType(String.class)
                    .valueType(Map.class)
                    .storeByReference(true),
                b -> b.name(cacheExifName)
                    .expireAfterWrite(cacheExifExpire, TimeUnit.SECONDS)
                    .entryCapacity(cacheExifSize)
                    .keyType(Integer.class));

    }

    protected static class GsOffsetDateTimeSerializer extends OffsetDateTimeSerializer {
        private static final long serialVersionUID = 1L;

        public GsOffsetDateTimeSerializer() {
            super(OffsetDateTimeSerializer.INSTANCE,
                false,
                false,
                DateTimeHelper.SPRING_VALUE_DATE_TIME_FORMATTER);
        }

    }

    @Bean
    @Primary
    public Jackson2ObjectMapperBuilderCustomizer jsonCustomizer() {
        return builder -> {
            builder.simpleDateFormat(DateTimeHelper.EXIF_DATE_TIME_PATTERN);
            builder.serializers(new LocalDateSerializer(DateTimeHelper.EXIF_VALUE_DATE_FORMATTER));
            builder.serializers(new LocalDateTimeSerializer(DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER));
            builder.serializers(new LocalDateTimeSerializer(DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER));
            builder.serializers(new GsOffsetDateTimeSerializer());
            builder.deserializers(new OffsetDateTimeDeserializer());

        };
    }

    @Bean(name = "hdfsFileSystem")
    public FileSystem hdfsFileSystem(
        @Value("${application.gs.principal}") String principal,
        @Value("${application.gs.keytab}") String keyTab
    ) {
        org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
        UserGroupInformation.setConfiguration(configuration);
        try {
            ApplicationConfig.LOGGER.info("Kerberos Login from login {} and keytab {}", principal, keyTab);

            UserGroupInformation.loginUserFromKeytab(principal, keyTab);
            ApplicationConfig.LOGGER.info("Kerberos Login from login {} and keytab {}", principal, keyTab);
        } catch (IOException e1) {
            ApplicationConfig.LOGGER
                .warn("Error when login {},{} : {}", principal, keyTab, ExceptionUtils.getStackTrace(e1));
            throw new RuntimeException(e1);
        }
        PrivilegedAction<FileSystem> action = () -> {
            FileSystem retValue = null;
            try {
                retValue = FileSystem.get(configuration);
            } catch (IOException e) {
                ApplicationConfig.LOGGER.warn(
                    "Error when Getting FileSystem {},{} : {}",
                    principal,
                    keyTab,
                    ExceptionUtils.getStackTrace(e));
                throw new RuntimeException(e);
            }
            return retValue;
        };
        try {
            return UserGroupInformation.getLoginUser()
                .doAs(action);
        } catch (IOException e) {
            ApplicationConfig.LOGGER
                .warn("Error when login {},{} : {}", principal, keyTab, ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    public static class OffsetDateTimeDeserializer extends JSR310DateTimeDeserializerBase<OffsetDateTime> {
        private static final DateTimeFormatter DEFAULT_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

        private static final long              serialVersionUID  = 1L;

        public OffsetDateTimeDeserializer() {
            super(OffsetDateTime.class,
                DateTimeHelper.EXIF_VALUE_DATE_TIME_FORMATTER);
        }

        @Override
        protected JSR310DateTimeDeserializerBase<OffsetDateTime> withShape(Shape shape) { return this; }

        @Override
        protected JSR310DateTimeDeserializerBase<OffsetDateTime> withDateFormat(DateTimeFormatter dtf) { return this; }

        @Override
        protected JSR310DateTimeDeserializerBase<OffsetDateTime> withLeniency(Boolean leniency) { return this; }

        @Override
        public OffsetDateTime deserialize(JsonParser parser, DeserializationContext context)
            throws IOException, JsonProcessingException {
            if (parser.hasTokenId(JsonTokenId.ID_STRING)) {
                String string = parser.getText()
                    .trim();
                if (string.length() == 0) {
                    if (!this.isLenient()) { return this._failForNotLenient(parser, context, JsonToken.VALUE_STRING); }
                    return null;
                }

                try {
                    return DateTimeHelper.toOffsetDateTime(string, DateTimeHelper.SPRING_VALUE_DATE_TIME_FORMATTER);
                } catch (DateTimeException e) {
                    return this._handleDateTimeException(context, e, string);
                }
            }
            if (parser.isExpectedStartArrayToken()) {
                JsonToken t = parser.nextToken();
                if (t == JsonToken.END_ARRAY) { return null; }
                if (((t == JsonToken.VALUE_STRING) || (t == JsonToken.VALUE_EMBEDDED_OBJECT))
                    && context.isEnabled(DeserializationFeature.UNWRAP_SINGLE_VALUE_ARRAYS)) {
                    final OffsetDateTime parsed = this.deserialize(parser, context);
                    if (parser.nextToken() != JsonToken.END_ARRAY) {
                        this.handleMissingEndArrayForSingle(parser, context);
                    }
                    return parsed;
                }
                if (t == JsonToken.VALUE_NUMBER_INT) {
                    OffsetDateTime result;

                    int year = parser.getIntValue();
                    int month = parser.nextIntValue(-1);
                    int day = parser.nextIntValue(-1);
                    int hour = parser.nextIntValue(-1);
                    int minute = parser.nextIntValue(-1);

                    t = parser.nextToken();
                    if (t == JsonToken.END_ARRAY) {
                        result = OffsetDateTime
                            .of(year, month, day, hour, minute, 0, 0, DateTimeHelper.ZONE_OFFSET_EUROPE_PARIS);
                    } else {
                        int second = parser.getIntValue();
                        t = parser.nextToken();
                        if (t == JsonToken.END_ARRAY) {
                            result = OffsetDateTime
                                .of(year, month, day, hour, minute, second, 0, DateTimeHelper.ZONE_OFFSET_EUROPE_PARIS);
                        } else {
                            int partialSecond = parser.getIntValue();
                            if ((partialSecond < 1_000)
                                && !context.isEnabled(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)) {
                                partialSecond *= 1_000_000; // value is milliseconds, convert it to nanoseconds
                            }
                            if (parser.nextToken() != JsonToken.END_ARRAY) {
                                throw context.wrongTokenException(
                                    parser,
                                    this.handledType(),
                                    JsonToken.END_ARRAY,
                                    "Expected array to end");
                            }
                            result = OffsetDateTime.of(
                                year,
                                month,
                                day,
                                hour,
                                minute,
                                second,
                                partialSecond,
                                DateTimeHelper.ZONE_OFFSET_EUROPE_PARIS);
                        }
                    }
                    return result;
                }
                context.reportInputMismatch(
                    this.handledType(),
                    "Unexpected token (%s) within Array, expected VALUE_NUMBER_INT",
                    t);
            }
            if (parser.hasToken(JsonToken.VALUE_EMBEDDED_OBJECT)) {
                return (OffsetDateTime) parser.getEmbeddedObject();
            }
            if (parser.hasToken(JsonToken.VALUE_NUMBER_INT)) {
                this._throwNoNumericTimestampNeedTimeZone(parser, context);
            }
            return this._handleUnexpectedToken(context, parser, "Expected array or string.");
        }

    }

    @Bean
    public FormattingConversionService conversionService() {
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService(false);

        DateTimeFormatterRegistrar registrar = new DateTimeFormatterRegistrar();
        registrar.setDateFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_PATTERN));
        registrar.setDateTimeFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_TIME_PATTERN));
        registrar.registerFormatters(conversionService);

        return conversionService;
    }

    @Bean
    public WebConversionService webConversionService() {
        WebConversionService conversionService = new WebConversionService(DateTimeHelper.SPRING_DATE_TIME_PATTERN) {

        };

        DateTimeFormatterRegistrar registrar = new DateTimeFormatterRegistrar();
        registrar.setDateFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_PATTERN));
        registrar.setDateTimeFormatter(DateTimeFormatter.ofPattern(DateTimeHelper.SPRING_DATE_TIME_PATTERN));
        registrar.registerFormatters(conversionService);

        return conversionService;
    }

}
