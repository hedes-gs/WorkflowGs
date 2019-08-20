package com.workflow.model;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

@Retention(RUNTIME)
@Target(FIELD)
public @interface Column {
	public String hbaseName();

	public boolean isPartOfRowkey() default false;

	public int rowKeyNumber() default Integer.MAX_VALUE;

	public Class<? extends ToByte<?>> toByte();

	public String columnFamily() default "";

	public int fixedWidth() default Integer.MAX_VALUE;
}
