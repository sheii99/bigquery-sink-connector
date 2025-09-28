package com.iblync.kafka.connect.bigquery.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Documented
@Retention(RetentionPolicy.CLASS) // annotation info is kept in bytecode
@Target({
    ElementType.METHOD,
    ElementType.PARAMETER,
    ElementType.FIELD,
    ElementType.LOCAL_VARIABLE
})
public @interface Nullable {
}