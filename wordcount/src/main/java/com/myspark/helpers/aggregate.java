package com.myspark.helpers;

import org.apache.spark.api.java.function.Function2;

public class aggregate implements Function2<Integer, Integer, Integer>{
	private static final long serialVersionUID = 1L;

	@Override
	public Integer call(Integer x, Integer y) throws Exception {
		return x + y;
	}

}
