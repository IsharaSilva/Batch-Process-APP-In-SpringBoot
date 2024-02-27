package com.synergensolutions.sbsservice.common.batch.writer;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.CompositeItemWriter;

public class CSVItemCompositeWriter<T> extends CompositeItemWriter<T> {

	public CSVItemCompositeWriter(List<ItemWriter<? super T>> itemWriters) {
		super();
		setDelegates(itemWriters);
	}

}