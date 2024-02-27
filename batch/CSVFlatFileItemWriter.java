package com.synergensolutions.sbsservice.reports.writer;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

public class CSVFlatFileItemWriter<T> extends FlatFileItemWriter<T> {

	private static final String DELIMITER = ";";

	public CSVFlatFileItemWriter(File outputFile, boolean append, String[] filedNames, String header, String footer) {
		super();
		setResource(new FileSystemResource(outputFile));
		setAppendAllowed(append);

		BeanWrapperFieldExtractor<T> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
		beanWrapperFieldExtractor.setNames(filedNames);

		DelimitedLineAggregator<T> delimitedLineAggregator = new DelimitedLineAggregator<>();
		delimitedLineAggregator.setDelimiter(DELIMITER);
		delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);

		setLineAggregator(delimitedLineAggregator);

		setHeaderCallback(new FlatFileHeaderCallback() {
			@Override
			public void writeHeader(Writer writer) throws IOException {
				writer.write("sep=;\n");
				writer.write(header);
			}
		});

		setFooterCallback(new FlatFileFooterCallback() {
			@Override
			public void writeFooter(Writer writer) throws IOException {
				writer.write(footer);
			}
		});
	}
}
