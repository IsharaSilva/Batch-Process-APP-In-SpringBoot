package com.synergensolutions.sbsservice.common.batch.processor;

import java.util.function.Function;

import org.springframework.batch.item.ItemProcessor;

public class CSVRowProcessor<I, O> implements ItemProcessor<I, O> {

	private final Function<I, O> itemProcessingMachanism;

	public CSVRowProcessor(Function<I, O> itemProcessingMachanism) {
		super();
		this.itemProcessingMachanism = itemProcessingMachanism;
	}

	@Override
	public O process(I item) throws Exception {
		return itemProcessingMachanism.apply(item);
	}

}
