package com.synergensolutions.sbsservice.reports.config;

import com.synergensolutions.sbsservice.common.batch.processor.CSVRowProcessor;
import com.synergensolutions.sbsservice.common.batch.writer.CSVItemCompositeWriter;
import com.synergensolutions.sbsservice.reports.dto.ClientOutputRow;
import com.synergensolutions.sbsservice.reports.mapper.ClientOutputRowMapper;
import com.synergensolutions.sbsservice.reports.reader.DBReader;
import com.synergensolutions.sbsservice.reports.utils.CommonConstants;
import com.synergensolutions.sbsservice.reports.writer.CSVFlatFileItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.io.File;
import java.util.Arrays;

@Configuration
public class ClientReportJobConfig {

	private static final String[] CLIENT_REPORT_FIELDS = new String[] { "limsId", "clinicName", "region",
			"contactPersons", "addresses", "phoneNumbers", "fax", "emails", "active", "subClients", "languages",
			"currencyCode", "reportPreference", "gdpr", "invoiceTemplate", "statementTemplate", "billingDuration",
			"billTo", "dueDatePeriod", "noOfBuckets", "bucketPeriod" };

	private final JobBuilderFactory jobBuilderFactory;
	private final StepBuilderFactory stepBuilderFactory;
	private final DataSource dataSource;

	public ClientReportJobConfig(final JobBuilderFactory jobBuilderFactory, final StepBuilderFactory stepBuilderFactory,
			final DataSource dataSource) {
		super();
		this.jobBuilderFactory = jobBuilderFactory;
		this.stepBuilderFactory = stepBuilderFactory;
		this.dataSource = dataSource;
	}

	@Bean
	public CSVRowProcessor<ClientOutputRow, ClientOutputRow> clientExportProcessor() {
		return new CSVRowProcessor<>(i -> i);
	}

	@Bean
	@StepScope
	public CSVFlatFileItemWriter<ClientOutputRow> clientOutputWriter(
			@Value(CommonConstants.OUTPUT_CSV_PATH_JOB_PARAM_EXPRESSION) String outputFileUrl) {
		File outputFile = new File(outputFileUrl);
		return new CSVFlatFileItemWriter<>(outputFile, true, CLIENT_REPORT_FIELDS, CommonConstants.CLIENT_HEADER_ROW,
				"");
	}

	@Bean(destroyMethod = "")
	@StepScope
	public DBReader<ClientOutputRow> clientOutputRowReader() {
		return new DBReader<>(dataSource, "SELECT * FROM client_report_view", new ClientOutputRowMapper());
	}

	@Bean
	public CSVItemCompositeWriter<ClientOutputRow> clientExportCompositeWriter() {
		return new CSVItemCompositeWriter<>(Arrays.asList(clientOutputWriter("")));
	}

	@Bean
	@JobScope
	public Step clientReportJobStep(@Value(CommonConstants.BATCH_SIZE_JOB_PARAM_EXPRESSION) Long batchSize) {
		return stepBuilderFactory.get("ClientReportJobStep")
				.<ClientOutputRow, ClientOutputRow>chunk(Math.toIntExact(batchSize)).reader(clientOutputRowReader())
				.processor(clientExportProcessor()).writer(clientExportCompositeWriter()).build();
	}

	@Bean
	@Qualifier("ClientReportJob")
	public Job clientReportJob(JobCompletionNotificationListener listener) {
		return jobBuilderFactory.get("ClientReportJob").incrementer(new RunIdIncrementer()).listener(listener)
				.start(clientReportJobStep(0L)).build();
	}

}
