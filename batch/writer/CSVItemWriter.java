package com.synergensolutions.sbsservice.common.batch.writer;

import java.util.List;
import java.util.function.Consumer;

import org.springframework.batch.item.ItemWriter;

public class CSVItemWriter<T> implements ItemWriter<T> {

	private final Consumer<List<? extends T>> writingMachanism;

	public CSVItemWriter(Consumer<List<? extends T>> writingMachanism) {
		super();
		this.writingMachanism = writingMachanism;
	}

	@Override
	public void write(List<? extends T> items) throws Exception {
		writingMachanism.accept(items);
	}

}