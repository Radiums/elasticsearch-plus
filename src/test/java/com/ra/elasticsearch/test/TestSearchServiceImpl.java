package com.ra.elasticsearch.test;

import com.ra.elasticsearch.service.AbstractSearchService;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
public class TestSearchServiceImpl extends AbstractSearchService<TestVO> implements TestSearchService {
    @Override
    protected String getIndexName() {
        return "test";
    }

    @Override
    protected List getListVOList(int offset, int pageSize) {
        TestVO testVO = new TestVO();
        testVO.setName("123");
        testVO.setAge(0);
        TestVO testVO2 = new TestVO();
        testVO2.setName("234");
        testVO2.setAge(0);
        return Arrays.asList(testVO, testVO2);
    }

    @Override
    public List getVOListById(String id) {
        TestVO testVO = new TestVO();
        testVO.setName("123");
        testVO.setAge(0);
        return Collections.singletonList(testVO);
    }
}
