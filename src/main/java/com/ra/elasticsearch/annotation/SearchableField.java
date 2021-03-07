package com.ra.elasticsearch.annotation;

import com.ra.elasticsearch.enums.ESAnalyzer;
import com.ra.elasticsearch.enums.ESType;

import java.lang.annotation.*;

/**
 * Created by leilei on 2016/12/28.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SearchableField {

    /**
     * es映射类型
     */
    ESType type() default ESType.KEYWORD;

    /**
     * es analyzer
     */
    ESAnalyzer analyzer() default ESAnalyzer.DEFAULT;

    String copyTo() default "";

    /**
     * 默认的排序字段
     */
    boolean isSortDefault() default false;
}
