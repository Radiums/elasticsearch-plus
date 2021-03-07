package com.ra.elasticsearch.annotation;

import java.lang.annotation.*;

/**
 * Created by lizhen on 2017/10/25.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SearchId {
}
