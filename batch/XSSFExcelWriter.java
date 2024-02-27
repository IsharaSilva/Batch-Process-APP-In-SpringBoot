package com.synergensolutions.sbsservice.reports.writer;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.synergensolutions.sbsservice.common.util.FileUtils;
import com.synergensolutions.sbsservice.reports.exception.GenerateReportException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class XSSFExcelWriter {
	private final Collection<String> headers;
	private final Collection<List<String>> data;
	private final String fileTitle;
	private final String fileName;

	public XSSFExcelWriter(final String fileName, final String fileTitle, final Collection<String> headers,
			final Collection<List<String>> data) {
		super();
		this.fileTitle = fileTitle;
		this.fileName = fileName;
		this.headers = headers;
		this.data = data;
	}

	public byte[] writer() {

		try (Workbook workbook = new XSSFWorkbook()) {

			Sheet sheet = workbook.createSheet(this.fileTitle.split(" \\(")[0]);

			// create title row
			AtomicInteger rowIndex = new AtomicInteger(0);
			Row titleRow = sheet.createRow(rowIndex.getAndIncrement());
			Cell titleCell = titleRow.createCell(0);
			titleCell.setCellValue(this.fileTitle);

			rowIndex.getAndIncrement();

			// create header row
			Row headerRow = sheet.createRow(rowIndex.getAndIncrement());

			AtomicInteger columnIndex = new AtomicInteger(0);
			headers.forEach(h -> {
				Cell headerCell = headerRow.createCell(columnIndex.getAndIncrement());
				headerCell.setCellValue(h);
			});

			// create data rows
			data.forEach(d -> {

				Row dataRow = sheet.createRow(rowIndex.getAndIncrement());
				AtomicInteger dataColumnIndex = new AtomicInteger(0);
				d.forEach(di -> {
					Cell headerCell = dataRow.createCell(dataColumnIndex.getAndIncrement());
					headerCell.setCellValue(di);
				});

			});

			File reportFile = FileUtils.createTempFile(fileName, ".xlsx");
			FileOutputStream outputStream = new FileOutputStream(reportFile);
			workbook.write(outputStream);

			return Files.readAllBytes(reportFile.toPath());

		} catch (Exception e) {
			log.error(e.getLocalizedMessage());
			throw new GenerateReportException(e.getLocalizedMessage());
		}
	}

}
