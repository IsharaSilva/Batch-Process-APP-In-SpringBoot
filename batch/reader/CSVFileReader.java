package com.synergensolutions.sbsservice.common.batch.reader;

import java.io.File;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.core.io.FileSystemResource;

public class CSVFileReader<T> extends FlatFileItemReader<T> {

	public CSVFileReader(File csvFile, LineMapper<T> lineMapper) {
		super();
		setResource(new FileSystemResource(csvFile));
		setLineMapper(lineMapper);
		setLinesToSkip(1);
	}

}
