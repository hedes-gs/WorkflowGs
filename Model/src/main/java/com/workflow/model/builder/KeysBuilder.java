package com.workflow.model.builder;

import java.nio.charset.Charset;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.workflow.model.ExchangedTiffData;
import com.workflow.model.HbaseExifData;
import com.workflow.model.HbaseExifDataOfImages;
import com.workflow.model.HbaseImageThumbnail;
import com.workflow.model.storm.FinalImage;

public class KeysBuilder {

    private static final HbaseExifDataKeyBuilder         HBASE_EXIF_DATA_KEY_BUILDER           = new HbaseExifDataKeyBuilder();
    private static final HbaseExifDataOfImagesKeyBuilder HBASE_EXIF_DATA_OF_IMAGES_KEY_BUILDER = new HbaseExifDataOfImagesKeyBuilder();

    public static TopicExifKeyBuilder topicExifKeyBuilder() { return new TopicExifKeyBuilder(); }

    public static TopicExifSizeOfImageStreamBuilder topicExifSizeOfImageStreamBuilder() {
        return new TopicExifSizeOfImageStreamBuilder();
    }

    public static HbaseExifDataOfImagesKeyBuilder hbaseExifDataOfImagesKeyBuilder() {
        return KeysBuilder.HBASE_EXIF_DATA_OF_IMAGES_KEY_BUILDER;
    }

    public static HbaseExifDataKeyBuilder hbaseExifDataKeyBuilder() { return KeysBuilder.HBASE_EXIF_DATA_KEY_BUILDER; }

    public static TopicTransformedThumbKeyBuilder topicTransformedThumbKeyBuilder() {
        return new TopicTransformedThumbKeyBuilder();
    }

    public static TopicImageDataToPersistKeyBuilder topicImageDataToPersistKeyBuilder() {
        return new TopicImageDataToPersistKeyBuilder();
    }

    public static TopicExifImageDataToPersistKeyBuilder topicExifImageDataToPersistKeyBuilder() {
        return new TopicExifImageDataToPersistKeyBuilder();
    }

    public static TopicFileKeyBuilder topicFileKeyBuilder() { return new TopicFileKeyBuilder(); }

    public static TopicThumbKeyBuilder topicThumbKeyBuilder() { return new TopicThumbKeyBuilder(); }

    public static String buildKeyForExifData(final String imageId, final short tag, final short[] etdPath) {
        HashFunction hf = Hashing.murmur3_128();
        String hbedoiHashCode = hf.newHasher()
            .putString(imageId, Charset.forName("UTf-8"))
            .putLong(tag)
            .putObject(etdPath, (path, sink) -> {
                for (short t : path) {
                    sink.putShort(t);
                }
            })
            .hash()
            .toString();
        return hbedoiHashCode;
    }

    public static class TopicThumbKey {
        protected final String  originalImageKey;
        protected final int     imgNumber;
        protected final short[] path;

        public TopicThumbKey(
            String originalImageKey,
            int imgNumber,
            short[] path
        ) {
            super();
            this.path = path;
            this.originalImageKey = originalImageKey;
            this.imgNumber = imgNumber;
        }

        public short[] getPath() { return this.path; }

        public String getOriginalImageKey() { return this.originalImageKey; }

        public int getImgNumber() { return this.imgNumber; }

    }

    public static class TopicExifKey {
        protected final String  originalImageKey;
        protected final int     tiffId;
        protected final short[] path;

        public TopicExifKey(
            String key,
            short[] path,
            int tiffId
        ) {
            super();
            this.originalImageKey = key;
            this.tiffId = tiffId;
            this.path = path;
        }

        public String getOriginalKey() { return this.originalImageKey; }

        public int getTiffId() { return this.tiffId; }

    }

    public static class TopicTransformedThumbKeyBuilder {
        protected String        originalImageKey;
        protected static String IMAGE_KEYWORD = "IMG";
        protected int           version;

        public TopicTransformedThumbKeyBuilder withOriginalImageKey(String key) {
            this.originalImageKey = key;
            return this;
        }

        public TopicTransformedThumbKeyBuilder withVersion(int version) {
            this.version = version;
            return this;
        }

        public String getOriginalKey(String key) {
            String[] tokens = key.split("\\-");
            return tokens[0];
        }

        public String build() {
            HashFunction hf = Hashing.murmur3_128();
            String hashcode = hf.newHasher()
                .putString(this.originalImageKey, Charset.forName("UTf-8"))
                .putInt(this.version)
                .hash()
                .toString();

            StringJoiner fruitJoiner = new StringJoiner("-");
            fruitJoiner.add(this.originalImageKey)
                .add(TopicThumbKeyBuilder.IMAGE_KEYWORD)
                .add(hashcode);
            return fruitJoiner.toString();
        }

    }

    public static class TopicThumbKeyBuilder {
        protected static String IMAGE_KEYWORD = "IMG";
        protected String        originalImageKey;
        protected int           thumbNb;
        private String          pathInExifTags;

        public TopicThumbKeyBuilder withOriginalImageKey(String key) {
            this.originalImageKey = key;
            return this;
        }

        public TopicThumbKeyBuilder withThumbNb(int thumbNb) {
            this.thumbNb = thumbNb;
            return this;
        }

        public String build() {
            StringJoiner fruitJoiner = new StringJoiner("-");
            fruitJoiner.add(this.originalImageKey)
                .add(TopicThumbKeyBuilder.IMAGE_KEYWORD)
                .add(this.pathInExifTags)
                .add(Integer.toHexString(this.thumbNb));
            return fruitJoiner.toString();
        }

        public TopicThumbKey build(String key) {
            String[] tokens = key.split("\\-");
            String[] shortTokensAsString = tokens[2].substring(1)
                .split("/");
            List<Short> pathList = Stream.of(shortTokensAsString)
                .map(Short::parseShort)
                .collect(Collectors.toList());
            short[] path = new short[pathList.size()];
            for (int k = 0; k < path.length; k++) {
                path[k] = pathList.get(k);
            }
            return new TopicThumbKey(tokens[0], Integer.parseUnsignedInt(tokens[3], 16), path);
        }

        public TopicThumbKeyBuilder withPathInExifTags(short[] path2) {
            StringJoiner joiner = new StringJoiner("/", "/", "");
            for (short node : path2) {
                joiner.add(Short.toString(node));
            }
            this.pathInExifTags = joiner.toString();
            return this;
        }

    }

    public static class TopicExifKeyBuilder {
        protected static String EXIF_KEYWORD = "EXIF";
        protected String        originalImageKey;
        protected String        path;
        protected int           tiffId;

        public TopicExifKeyBuilder withOriginalImageKey(String key) {
            this.originalImageKey = key;
            return this;
        }

        public TopicExifKeyBuilder withTiffId(int tiffId) {
            this.tiffId = tiffId;
            return this;
        }

        public String build() {
            StringJoiner fruitJoiner = new StringJoiner("-");
            fruitJoiner.add(this.originalImageKey)
                .add(TopicExifKeyBuilder.EXIF_KEYWORD)
                .add(this.path)
                .add(Integer.toHexString(this.tiffId));
            return fruitJoiner.toString();
        }

        public TopicExifKey build(String key) {
            String[] tokens = key.split("\\-");
            String originalImageKey = tokens[0];
            String[] shortTokensAsString = tokens[2].substring(1)
                .split("/");
            List<Short> pathList = Stream.of(shortTokensAsString)
                .map((i) -> (short) Integer.parseUnsignedInt(i, 16))
                .collect(Collectors.toList());
            short[] path = new short[pathList.size()];
            for (int k = 0; k < path.length; k++) {
                path[k] = pathList.get(k);
            }
            int tiffId = Integer.parseUnsignedInt(tokens[3], 16);

            return new TopicExifKey(originalImageKey, path, tiffId);
        }

        private TopicExifKeyBuilder() {}

        public TopicExifKeyBuilder withPath(short[] path2) {
            StringJoiner joiner = new StringJoiner("/", "/", "");
            for (short node : path2) {
                joiner.add(Integer.toHexString(node));
            }
            this.path = joiner.toString();
            return this;
        }

    }

    public static class TopicExifSizeOfImageStreamBuilder {
        protected String key;
        protected String widthOrEight;

        public TopicExifSizeOfImageStreamBuilder withKey(String key) {
            this.key = key;
            return this;
        }

        public TopicExifSizeOfImageStreamBuilder withSizeWidth() {
            this.widthOrEight = "WIDTH";
            return this;
        }

        public TopicExifSizeOfImageStreamBuilder withSizeHeight() {
            this.widthOrEight = "HEIGHT";
            return this;
        }

        public TopicExifSizeOfImageStreamBuilder withCreationDate() {
            this.widthOrEight = "CREATION_DATE";
            return this;
        }

        public String build() {
            StringJoiner fruitJoiner = new StringJoiner("-");
            fruitJoiner.add(this.key)
                .add(this.widthOrEight);
            return fruitJoiner.toString();
        }

        public String getOriginalKey(String key) {
            String[] tokens = key.split("\\-");
            return tokens[0];
        }
    }

    public static class HbaseExifDataOfImagesKeyBuilder {
        public static String build(HbaseExifDataOfImages hbeodi) {
            HashFunction hf = Hashing.murmur3_128();
            String hbedoiHashCode = hf.newHasher()
                .putString(HbaseExifDataOfImages.class.getName(), Charset.forName("UTf-8"))
                .putString(hbeodi.getImageId(), Charset.forName("UTf-8"))
                .putLong(hbeodi.getExifTag())
                .putObject(hbeodi.getExifPath(), (path, sink) -> {
                    for (short t : path) {
                        sink.putShort(t);
                    }
                })
                .hash()
                .toString();
            return hbedoiHashCode;
        }
    }

    public static class HbaseExifDataKeyBuilder {
        public static String build(HbaseExifData hbeodi) {
            HashFunction hf = Hashing.murmur3_128();
            String hbedoiHashCode = hf.newHasher()
                .putString(hbeodi.getImageId(), Charset.forName("UTf-8"))
                .putLong(hbeodi.getExifTag())
                .putObject(hbeodi.getExifPath(), (path, sink) -> {
                    for (short t : path) {
                        sink.putShort(t);
                    }
                })
                .hash()
                .toString();
            return hbedoiHashCode;
        }
    }

    public static class HbaseImageThumbnailKeyBuilder {
        public static String build(HbaseImageThumbnail hbeodi) {
            HashFunction hf = Hashing.murmur3_128();
            String hbedoiHashCode = hf.newHasher()
                .putString(hbeodi.getImageId(), Charset.forName("UTf-8"))
                .putString(hbeodi.getPath(), Charset.forName("UTf-8"))
                .putString(hbeodi.getThumbName(), Charset.forName("UTf-8"))
                .hash()
                .toString();
            return hbedoiHashCode;
        }
    }

    public static class FinalImageKeyBuilder {
        public static String build(FinalImage hbeodi, String version) {
            HashFunction hf = Hashing.murmur3_128();
            String hbedoiHashCode = hf.newHasher()
                .putString(hbeodi.getDataId(), Charset.forName("UTf-8"))
                .putString(version, Charset.forName("UTf-8"))
                .putBytes(hbeodi.getCompressedImage())
                .hash()
                .toString();
            return hbedoiHashCode;
        }
    }

    public static class ExchangedTiffDataKeyBuilder {
        public static String build(ExchangedTiffData etd) {
            final String imageId = etd.getImageId();
            final short tag = etd.getTag();
            final short[] etdPath = etd.getPath();
            String hbedoiHashCode = KeysBuilder.buildKeyForExifData(imageId, tag, etdPath);
            return hbedoiHashCode;
        }
    }

    public static class TopicImageDataToPersistKeyBuilder {
        protected String originalImageKey;
        protected int    version;

        public TopicImageDataToPersistKeyBuilder withOriginalImageKey(String key) {
            this.originalImageKey = key;
            return this;
        }

        public TopicImageDataToPersistKeyBuilder withVersion(int version) {
            this.version = version;
            return this;
        }

        public static String getOriginalKey(String key) {
            String[] tokens = key.split("\\-");
            return tokens[0];
        }

        public String build() {
            HashFunction hf = Hashing.murmur3_128();
            String hashcode = hf.newHasher()
                .putString(this.originalImageKey, Charset.forName("UTf-8"))
                .putInt(this.version)
                .hash()
                .toString();

            StringJoiner fruitJoiner = new StringJoiner("-");
            fruitJoiner.add(this.originalImageKey)
                .add(TopicThumbKeyBuilder.IMAGE_KEYWORD)
                .add(hashcode);
            return fruitJoiner.toString();
        }
    }

    public static class TopicExifImageDataToPersistKeyBuilder {
        protected static String EXIF_KEYWORD = "EXIF";

        protected String        originalImageKey;
        protected short[]       exifPath;
        protected long          exifTag;

        public TopicExifImageDataToPersistKeyBuilder withOriginalImageKey(String key) {
            this.originalImageKey = key;
            return this;
        }

        public TopicExifImageDataToPersistKeyBuilder withExifPath(short[] exifPath) {
            this.exifPath = exifPath;
            return this;
        }

        public TopicExifImageDataToPersistKeyBuilder withExifTag(long exifTag) {
            this.exifTag = exifTag;
            return this;
        }

        public String build() {
            HashFunction hf = Hashing.murmur3_128();
            String hashcode = hf.newHasher()
                .putString(this.originalImageKey, Charset.forName("UTf-8"))
                .putLong(this.exifTag)
                .putObject(this.exifPath, (path, sink) -> {
                    for (short t : path) {
                        sink.putShort(t);
                    }
                })
                .hash()
                .toString();

            StringJoiner fruitJoiner = new StringJoiner("-");
            fruitJoiner.add(this.originalImageKey)
                .add(TopicExifImageDataToPersistKeyBuilder.EXIF_KEYWORD)
                .add(hashcode);
            return fruitJoiner.toString();
        }
    }

    public static class TopicFileKeyBuilder {
        protected String fileName;
        protected String filePath;

        public TopicFileKeyBuilder withFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        public TopicFileKeyBuilder withFilePath(String filePath) {
            this.filePath = filePath;
            return this;
        }

        public String build() {
            StringJoiner joiner = new StringJoiner("-");
            return joiner.add(this.filePath)
                .add(this.fileName)
                .toString();
        }

    }
}