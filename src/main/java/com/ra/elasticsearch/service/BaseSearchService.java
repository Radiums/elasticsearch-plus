package com.ra.elasticsearch.service;

import java.util.List;

public interface BaseSearchService {

    void initIndex(String indexSuffix);

    void index(String id);

    void delete(String id);

    void batchDelete(List<String> idList);

}
