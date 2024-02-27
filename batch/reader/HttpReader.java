package com.synergensolutions.sbsservice.common.batch.reader;

import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;

public class HttpReader<T> implements ItemReader<T> {

	private final Supplier<T> readingMachanism;

	public HttpReader(final Supplier<T> readingMachanism) {
		super();
		this.readingMachanism = readingMachanism;

	}

	@Override
	public T read() throws Exception {
		return this.readingMachanism.get();
	}

}