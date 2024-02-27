package com.synergensolutions.sbsservice.common.batch.utils;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

public class CSVDelimitedLineTokenizer extends DelimitedLineTokenizer {

	public CSVDelimitedLineTokenizer(String[] csvHeaders) {
		super();
		setNames(csvHeaders);
	}

}
