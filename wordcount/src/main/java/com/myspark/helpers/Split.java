package com.myspark.helpers;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;

public class Split implements FlatMapFunction<String, String>{
	private static final long serialVersionUID = 1L;

	private String separate;
	
	public Split(String s) {
		separate = s;
	}
	@Override
	public Iterable<String> call(String str) throws Exception {
		return Arrays.asList(str.split(separate));
	}
}
