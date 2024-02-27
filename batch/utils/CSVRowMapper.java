package com.synergensolutions.sbsservice.common.batch.utils;

import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;

public class CSVRowMapper<T> extends DefaultLineMapper<T> {

	public CSVRowMapper(AbstractLineTokenizer delimitedLineTokenizer, FieldSetMapper<T> fieldSetMapper) {
		super();
		setLineTokenizer(delimitedLineTokenizer);
		setFieldSetMapper(fieldSetMapper);
	}

}
