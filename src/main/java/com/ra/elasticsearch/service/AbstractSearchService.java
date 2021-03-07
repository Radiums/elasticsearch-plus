package com.ra.elasticsearch.service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ra.elasticsearch.annotation.SearchId;
import com.ra.elasticsearch.config.ElasticsearchProperties;
import com.ra.elasticsearch.utils.IndexBuildUtil;
import org.apache.commons.codec.digest.Md5Crypt;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractSearchService<T> implements BaseSearchService {

    private final static String INDEX_TYPE = "type";
    private final static int MAX_PAGE_SIZE = 500;
    private final static int BATCH_SIZE = 150000;
    @Autowired
    protected RestHighLevelClient client;
    private Logger logger = LoggerFactory.getLogger(AbstractSearchService.class);
    @Autowired
    private ElasticsearchProperties elasticsearchProperties;
    private Field searchIdField;
    private Class<T> voClazz;
    private WriteRequest.RefreshPolicy refreshPolicy;

    @PostConstruct
    public void init() {
        refreshPolicy = WriteRequest.RefreshPolicy.parse(elasticsearchProperties.getRefreshPolicy());
    }

    protected abstract String getIndexName();

    protected Class<T> getClazz() {
        if (voClazz != null) {
            return voClazz;
        }

        Type genType = getClass().getGenericSuperclass();
        Type[] params = ((ParameterizedType) genType).getActualTypeArguments();
        voClazz = (Class<T>) params[0];
        return voClazz;
    }

    /**
     * 初始化索引使用VO列表
     */
    protected abstract List<T> getListVOList(int offset, int pageSize);

    protected String getSearchId(T t) {
        try {
            Field field = getSearchIdField();
            PropertyDescriptor propertyDescriptor = BeanUtils.getPropertyDescriptor(t.getClass(), field.getName());
            Method readMethod = propertyDescriptor.getReadMethod();
            Object invoke = readMethod.invoke(t);
            String idStr = invoke.toString();
            byte[] bytes = idStr.getBytes();
            if (bytes.length > 512) {
                idStr = Md5Crypt.md5Crypt(bytes);
            }
            return idStr;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    private Field getSearchIdField() {
        if (searchIdField != null) {
            return searchIdField;
        }

        List<Field> fieldList = new ArrayList<>();
        Class tempClass = getClazz();
        while (tempClass != null) {
            fieldList.addAll(Arrays.asList(tempClass.getDeclaredFields()));
            tempClass = tempClass.getSuperclass();
        }

        for (Field field : fieldList) {
            if (field.isAnnotationPresent(SearchId.class)) {
                searchIdField = field;
                return searchIdField;
            }
        }
        throw new IllegalArgumentException(voClazz == null ? null : voClazz.getName() + " must have a searchId field");
    }

    /**
     * 索引单个VO
     */
    public abstract List<T> getVOListById(String id);

    public List<T> transformSearchResultData(SearchResponse searchResponse) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        return transformSearchResultData(hits);
    }

    public List<T> transformSearchResultDataWithHighlight(SearchResponse searchResponse, List<String> highlightFieldNames) {
        SearchHit[] hits = searchResponse.getHits().getHits();
        return transformSearchResultDataWithHighlight(hits, highlightFieldNames);
    }

    public List<T> transformSearchResultData(SearchHit[] hits) {
        List<T> ret = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            String sourceString = hit.getSourceAsString();
            T vo = JSONObject.parseObject(sourceString, getClazz());
            ret.add(vo);
        }
        return ret;
    }

    public List<T> transformSearchResultDataWithHighlight(SearchHit[] hits, List<String> highlightFieldNames) {
        List<T> ret = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            try {
                String sourceString = hit.getSourceAsString();
                T vo = JSONObject.parseObject(sourceString, getClazz());

                BeanInfo beanInfo = Introspector.getBeanInfo(vo.getClass());
                PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
                Map<String, Method> fieldNameWriteMethodMap =
                        Stream.of(propertyDescriptors).filter(propertyDescriptor -> propertyDescriptor.getPropertyType() != Class.class)
                                .collect(Collectors.toMap(PropertyDescriptor::getName, PropertyDescriptor::getWriteMethod));

                if (!CollectionUtils.isEmpty(highlightFieldNames)) {
                    for (String highlightFieldName : highlightFieldNames) {
                        HighlightField highlightField = hit.getHighlightFields().get(highlightFieldName);
                        if (highlightField != null) {
                            String highlight = highlightField.getFragments()[0].string();
                            Method method = fieldNameWriteMethodMap.get(highlightFieldName);
                            if (method != null) {
                                method.invoke(vo, highlight);
                            }
                        }
                    }
                }
                ret.add(vo);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        return ret;
    }

    @Override
    public void initIndex(String indexSuffix) {
        ExecutorService executorService = Executors.newFixedThreadPool(elasticsearchProperties.getConcurrencyLevel());
        String indexNameAlias = getIndexName();
        String indexRealName = indexNameAlias + indexSuffix;
        try {
            IndexBuildUtil.reInitMapping(indexNameAlias, INDEX_TYPE, getClazz(), client, indexSuffix);
            int pageSize = MAX_PAGE_SIZE;
            CountDownLatch countDownLatch = new CountDownLatch(elasticsearchProperties.getConcurrencyLevel());
            for (int threadIndex = 0; threadIndex < elasticsearchProperties.getConcurrencyLevel(); threadIndex++) {
                int finalThreadIndex = threadIndex;
                executorService.execute(() -> {
                    try {
                        for (int i = 0; ; i++) {
                            long version = System.currentTimeMillis();
                            int offset = finalThreadIndex * BATCH_SIZE + i * pageSize;
                            List<T> listVOS = getListVOList(offset, pageSize);
                            if (CollectionUtils.isEmpty(listVOS) || offset >= (finalThreadIndex + 1) * BATCH_SIZE) {
                                break;
                            }
                            BulkRequest bulkRequest = new BulkRequest();
                            Set<String> idSet = new HashSet<>();
                            for (T listVO : listVOS) {
                                IndexRequest indexRequest = new IndexRequest(indexRealName);
                                String id = getSearchId(listVO);
                                if (idSet.contains(id)) {
                                    continue;
                                }
                                idSet.add(id);
                                indexRequest.id(id)
                                        .source(JSONObject.toJSONString(listVO, SerializerFeature.DisableCircularReferenceDetect), XContentType.JSON)
                                        .versionType(VersionType.EXTERNAL).version(version);
                                bulkRequest.add(indexRequest);
                            }

                            BulkResponse bulkResponse = null;
                            bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                            if (bulkResponse != null && bulkResponse.hasFailures()) {
                                Stream.of(bulkResponse.getItems()).forEach(x -> {
                                    if (x.isFailed()) {
                                        logger.error("[ES BULK ERROR] index: {},errorMsg:{}", getIndexName(), x.getFailureMessage());
                                    }
                                });
                            }
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    } finally {
                        countDownLatch.countDown();
                        logger.info("[ES BULK COUNT DOWN]-{}", countDownLatch.getCount());
                    }
                });
            }
            countDownLatch.await();
            updateInitIndexSetting(indexNameAlias, indexSuffix);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            executorService.shutdown();
        }
    }

    @Override
    public void index(String id) {
        try {
            List<T> voList = getVOListById(id);
            if (CollectionUtils.isEmpty(voList)) {
                logger.error(" index: {},listVO is not exist, id: {}", getIndexName(), id);
                return;
            }
            for (T listVO : voList) {
                IndexRequest indexRequest = new IndexRequest(getIndexName());
                indexRequest.id(getSearchId(listVO)).setRefreshPolicy(refreshPolicy)
                        .source(JSONObject.toJSONString(listVO, SerializerFeature.DisableCircularReferenceDetect), XContentType.JSON)
                        .versionType(VersionType.EXTERNAL).version(System.currentTimeMillis());
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(" index: {},id: {} ,resp status {}", getIndexName(), id, indexResponse.status().getStatus());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void delete(String id) {
        try {
            DeleteRequest deleteRequest = new DeleteRequest(getIndexName(), id).setRefreshPolicy(refreshPolicy);
            client.delete(deleteRequest, RequestOptions.DEFAULT);
            logger.warn("delete index: {},id: {}", getIndexName(), id);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public void batchDelete(List<String> idList) {
        if (CollectionUtils.isEmpty(idList)) { return; }
        try {
            BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(refreshPolicy);
            for (String id : idList) {
                DeleteRequest deleteRequest = new DeleteRequest(getIndexName(), id);
                bulkRequest.add(deleteRequest);
            }
            BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info("bulk delete index: {}, result-{},use-{}", getIndexName(), bulkResponse.hasFailures(), bulkResponse.getIngestTookInMillis());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void updateInitIndexSetting(String indexName, String indexSuffix) throws Exception {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(indexName);
        boolean exists = client.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (exists) {
            GetAliasesRequest getAliasesRequest = new GetAliasesRequest();
            getAliasesRequest.aliases(indexName);
            GetAliasesResponse getAliasesResponse = client.indices().getAlias(getAliasesRequest, RequestOptions.DEFAULT);
            Map<String, Set<AliasMetaData>> aliasesMap = getAliasesResponse.getAliases();
            if (aliasesMap.isEmpty()) {
                logger.info("delete old index-{}", indexName);
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
                client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
                aliasAction.index(indexName + indexSuffix).alias(indexName);
                indicesAliasesRequest.addAliasAction(aliasAction);
                client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
            } else {
                String indexOriginName = aliasesMap.keySet().iterator().next();
                if (indexOriginName == null) {
                    throw new IllegalArgumentException("can not find index origin name by alias");
                }
                IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
                IndicesAliasesRequest.AliasActions removeAction =
                        new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
                removeAction.index(indexOriginName).alias(indexName);
                IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
                aliasAction.index(indexName + indexSuffix).alias(indexName);
                indicesAliasesRequest.addAliasAction(removeAction);
                indicesAliasesRequest.addAliasAction(aliasAction);
                client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
                DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexOriginName);
                client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            }
        } else {
            IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
            IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
            aliasAction.index(indexName + indexSuffix).alias(indexName);
            indicesAliasesRequest.addAliasAction(aliasAction);
            client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        }

        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);
        updateSettingsRequest.settings(Settings.builder().put("index.refresh_interval", elasticsearchProperties.getRefreshInterval()));
        AcknowledgedResponse acknowledgedResponse = client.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
        if (!acknowledgedResponse.isAcknowledged()) {
            logger.error("update index setting error -{}", indexName);
            throw new RuntimeException();
        }
        logger.info("update index setting-{}", indexName);
    }

}
