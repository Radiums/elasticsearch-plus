package com.ra.elasticsearch.utils;

import com.ra.elasticsearch.annotation.SearchableField;
import com.ra.elasticsearch.enums.ESAnalyzer;
import com.ra.elasticsearch.enums.ESType;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;

/**
 * Created by leilei on 2017/1/4.
 */
public class IndexBuildUtil {
    public static final String PROPERTIES_STRING = "properties";
    public static final String TYPE_STRING = "type";
    public static final String NESTED_STRING = "nested";
    public static final String ANALYZER_STRING = "analyzer";
    public static final String SEARCH_ANALYZER_STRING = "search_analyzer";
    public static final String TEXT_STRING = "text";
    public static final String RAW_STRING = "raw";
    public static final String RAW_LOWER_STRING = "raw_lower";
    public static final String PIN_YIN_STRING = "pinyin";
    public static final String PIN_YIN_RAW_STRING = "pinyin_raw";
    public static final String FIELDS_STRING = "fields";
    public static final String FIELDDATA_STRING = "fielddata";
    public static final String TERM_VECTOR_STRING = "term_vector";
    public static final String WITH_POSITIONS_OFFSETS_STRING = "with_positions_offsets";
    public static final String FORMAT_STRING = "format";
    public static final String DATE_FORMAT_SUFFIX = " || epoch_millis";
    public static final String HIGHLIGHT_PRE_TAG = "\u200b";
    public static final String HIGHLIGHT_POST_TAG = "\u200c";
    public static final String COPY_TO_STRING = "copy_to";
    private static final Logger logger = LoggerFactory.getLogger(IndexBuildUtil.class);

    private IndexBuildUtil() {
    }

    public static void reInitMapping(String indexName, String indexType, Class clazz, RestHighLevelClient client, String indexSuffix) {
        createIndex(client, indexName, indexSuffix);
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            xContentBuilder.startObject();
            xContentBuilder.startObject(PROPERTIES_STRING);

            Field[] declaredFields = getAllClassFields(clazz, null);
            buildFields(declaredFields, xContentBuilder);

            xContentBuilder.endObject();
            xContentBuilder.endObject();
            if (logger.isDebugEnabled()) {
                logger.debug("mapping for {} is {}", indexName + indexSuffix, xContentBuilder.getOutputStream().toString());
            }
            PutMappingRequest putMappingRequest = new PutMappingRequest(indexName + indexSuffix).type(indexType).source(xContentBuilder);
            client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void createIndex(RestHighLevelClient client, String indexName, String indexSuffix) {
        String newIndexName = indexName + indexSuffix;
        try {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(newIndexName);
            CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            if (logger.isDebugEnabled()) {
                logger.debug("result for create index-{} is {}", newIndexName, createIndexResponse.isAcknowledged());
            }
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private static void buildListTypeContent(XContentBuilder xContentBuilder, Field field, String fieldName) throws IOException {
        Class<?> tempType = field.getType();
        if (tempType == List.class || tempType == Set.class) {
            ParameterizedType genericType = (ParameterizedType) field.getGenericType();
            Type[] actualTypeArguments = genericType.getActualTypeArguments();
            Class actualTypeArgument = (Class) actualTypeArguments[0];
            if (actualTypeArgument == String.class) {
                xContentBuilder.startObject(fieldName);
                xContentBuilder.field(TYPE_STRING, "keyword".toLowerCase());
                xContentBuilder.endObject();
            } else {
                Field[] declaredFields = buildSuperClassFields(actualTypeArgument);
                xContentBuilder.startObject(fieldName);
                xContentBuilder.startObject(PROPERTIES_STRING);
                buildFields(declaredFields, xContentBuilder);
                xContentBuilder.endObject();
                xContentBuilder.endObject();
            }
        } else {
            throw new IllegalArgumentException(String.format("field type error %s,type %s", field.getName(), tempType.getName()));
        }
    }

    private static void buildObjectTypeContent(XContentBuilder xContentBuilder, Field field, String fieldName) throws IOException {
        Class<?> type = field.getType();
        Field[] declaredFields = type.getDeclaredFields();
        xContentBuilder.startObject(fieldName);
        xContentBuilder.startObject(PROPERTIES_STRING);
        buildFields(declaredFields, xContentBuilder);
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    private static Field[] buildSuperClassFields(Class actualTypeArgument) {
        Field[] fields = null;
        Field[] declaredFields = actualTypeArgument.getDeclaredFields();
        Type genericSuperclass = actualTypeArgument.getGenericSuperclass();
        if (genericSuperclass != Object.class) {
            Field[] superClassField = ((Class) genericSuperclass).getDeclaredFields();
            fields = ArrayUtils.concat(declaredFields, superClassField, Field.class);
            buildSuperClassFields((Class) genericSuperclass);
        } else {
            fields = declaredFields;
        }
        return fields;
    }

    private static Field[] getAllClassFields(Class clazz, Field[] declaredFields) {
        if (declaredFields == null) {
            declaredFields = clazz.getDeclaredFields();
        } else {
            declaredFields = ArrayUtils.concat(declaredFields, clazz.getDeclaredFields(), Field.class);
        }
        Class superclass = clazz.getSuperclass();
        if (superclass != Object.class) {
            declaredFields = getAllClassFields(superclass, declaredFields);
        }
        return declaredFields;
    }

    private static void buildPinYinField(XContentBuilder xContentBuilder, String fieldName) throws IOException {
        xContentBuilder.startObject(fieldName);
        String name = ESType.TEXT.name();
        xContentBuilder.field(TYPE_STRING, name.toLowerCase());
        xContentBuilder.startObject(FIELDS_STRING);
        xContentBuilder.startObject(TEXT_STRING);
        xContentBuilder.field(TYPE_STRING, name.toLowerCase());
        xContentBuilder.field(ANALYZER_STRING, ESAnalyzer.STANDARD.name().toLowerCase());
        xContentBuilder.endObject();
        xContentBuilder.startObject(PIN_YIN_STRING);
        xContentBuilder.field(TYPE_STRING, name.toLowerCase());
        xContentBuilder.field(ANALYZER_STRING, ESAnalyzer.PINYIN.name().toLowerCase());
        xContentBuilder.endObject();
        xContentBuilder.startObject(RAW_STRING);
        xContentBuilder.field(TYPE_STRING, ESType.KEYWORD.name().toLowerCase());
        xContentBuilder.endObject();
        xContentBuilder.startObject(PIN_YIN_RAW_STRING);
        xContentBuilder.field(TYPE_STRING, ESType.TEXT.name().toLowerCase());
        xContentBuilder.field(ANALYZER_STRING, ESAnalyzer.PINYIN_RAW.name().toLowerCase());
        xContentBuilder.field(FIELDDATA_STRING, true);
        xContentBuilder.endObject();
        xContentBuilder.startObject(RAW_LOWER_STRING);
        xContentBuilder.field(TYPE_STRING, name.toLowerCase());
        xContentBuilder.field(ANALYZER_STRING, ESAnalyzer.RAW_LOWER.name().toLowerCase());
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    private static void buildTextFieldsByAnalyzer(XContentBuilder xContentBuilder, String fieldName, ESAnalyzer analyzer) throws IOException {
        xContentBuilder.startObject(fieldName);
        String name = ESType.TEXT.name();
        xContentBuilder.field(TYPE_STRING, name.toLowerCase());
        xContentBuilder.startObject(FIELDS_STRING);
        xContentBuilder.startObject(TEXT_STRING);
        xContentBuilder.field(TYPE_STRING, name.toLowerCase());
        xContentBuilder.field(ANALYZER_STRING, analyzer.name().toLowerCase());
        xContentBuilder.endObject();
        xContentBuilder.startObject(RAW_STRING);
        xContentBuilder.field(TYPE_STRING, ESType.KEYWORD.name().toLowerCase());
        xContentBuilder.endObject();
        xContentBuilder.endObject();
        xContentBuilder.endObject();
    }

    private static void buildFields(Field[] fields, XContentBuilder xContentBuilder) throws IOException {
        for (Field field : fields) {
            if (field == null) {
                continue;
            }
            String fieldName = field.getName();

            SearchableField annotation = field.getAnnotation(SearchableField.class);
            if (annotation == null) {
                continue;
            }
            ESType esType = annotation.type();
            ESAnalyzer analyzer = annotation.analyzer();

            switch (esType) {
                case OBJECT: {
                    buildObjectTypeContent(xContentBuilder, field, fieldName);
                    break;
                }
                case LIST: {
                    buildListTypeContent(xContentBuilder, field, fieldName);
                    break;
                }
                case TEXT: {
                    if (analyzer == ESAnalyzer.PINYIN) {
                        buildPinYinField(xContentBuilder, fieldName);
                    } else {
                        if (analyzer == ESAnalyzer.DEFAULT) {
                            xContentBuilder.startObject(fieldName);
                            xContentBuilder.field(TYPE_STRING, esType.name().toLowerCase());
                            String copyToField = annotation.copyTo();
                            if (!StringUtils.isEmpty(copyToField)) {
                                xContentBuilder.field(COPY_TO_STRING, copyToField);
                            }
                            xContentBuilder.startObject(FIELDS_STRING);
                            xContentBuilder.startObject(RAW_STRING);
                            xContentBuilder.field(TYPE_STRING, ESType.KEYWORD.name().toLowerCase());
                            xContentBuilder.endObject();
                            xContentBuilder.endObject();
                            xContentBuilder.endObject();
                        } else {
                            buildTextFieldsByAnalyzer(xContentBuilder, fieldName, analyzer);
                        }
                    }
                    break;
                }
                case DATE: {
                    xContentBuilder.startObject(fieldName);
                    xContentBuilder.field(TYPE_STRING, esType.name().toLowerCase());
                    xContentBuilder.field(FORMAT_STRING, "yyyy-MM-dd HH:mm:ss" + "||" + "yyyy-MM-dd" + DATE_FORMAT_SUFFIX);
                    xContentBuilder.endObject();
                    break;
                }
                default: {
                    xContentBuilder.startObject(fieldName);
                    xContentBuilder.field(TYPE_STRING, esType.name().toLowerCase());
                    if (analyzer != ESAnalyzer.DEFAULT) {
                        xContentBuilder.field(ANALYZER_STRING, analyzer.name().toLowerCase());
                    }
                    xContentBuilder.endObject();
                }
            }
        }
    }
}
