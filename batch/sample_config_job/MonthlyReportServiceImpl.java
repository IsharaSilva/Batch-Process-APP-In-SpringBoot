package com.synergensolutions.sbsservice.reports.service.impl;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.synergensolutions.sbsservice.clients.dto.ClientOutputDTO;
import com.synergensolutions.sbsservice.clients.service.ClientService;
import com.synergensolutions.sbsservice.reports.factory.AbstractMonthlyReportFactory;
import com.synergensolutions.sbsservice.reports.model.enums.ASPReportType;
import com.synergensolutions.sbsservice.reports.utils.CommonConstants;
import com.synergensolutions.sbsservice.statements.utils.FileNameUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.amazonaws.services.s3.AmazonS3;
import com.synergensolutions.sbsservice.common.aws.s3.S3Wrapper;
import com.synergensolutions.sbsservice.common.util.FileUtils;
import com.synergensolutions.sbsservice.messages.notifications.model.Notification;
import com.synergensolutions.sbsservice.messages.notifications.model.Topic;
import com.synergensolutions.sbsservice.messages.notifications.sender.NotificationSender;
import com.synergensolutions.sbsservice.reports.exception.GenerateReportException;
import com.synergensolutions.sbsservice.reports.factory.impl.DoctrixClientARReportFactory;
import com.synergensolutions.sbsservice.reports.factory.impl.DoctrixPaymentReportFactory;
import com.synergensolutions.sbsservice.reports.model.DownloadReportEvent;
import com.synergensolutions.sbsservice.reports.service.DownloadReportEventService;
import com.synergensolutions.sbsservice.reports.service.MonthlyReportService;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class MonthlyReportServiceImpl implements MonthlyReportService {

	private static final String TEMPLATE_EXTENSION = ".xls";
	private static final String FILE_EXTENSION_CSV = ".csv";
	private final DoctrixClientARReportFactory doctrixClientARReportFactory;
	private final DoctrixPaymentReportFactory doctrixPaymentReportFactory;
	private final DownloadReportEventService downloadReportEventService;
	private final ClientService clientService;
	private final S3Wrapper s3Wrapper;
	private final NotificationSender notificationSender;
	private final String s3ReportsPath;
	private final int batchSize;

	private final Job feeScheduleReportJob;
	private final Job monthlyArReportJob;
	private final Job invoiceReportJob;
	private final Job invoiceItemReportJob;
	private final Job writeOffReportJob;
	private final Job clinicPaymentReportJob;
	private final Job clientReportJob;
	private final Job topUpRequestReportJob;
	private final Job walletReportJob;
	private final Job monthlyASPReportJob;
	private final Job adjustmentWriteOffReportJob;

	private final JobLauncher jobLauncher;

	public MonthlyReportServiceImpl(final DoctrixClientARReportFactory doctrixClientARReportFactory,
			final DoctrixPaymentReportFactory doctrixPaymentReportFactory,
			final DownloadReportEventService downloadReportEventService, final ClientService clientService,
			final AmazonS3 amazonS3, @Value("${cloud.aws.s3.bucket}") final String bucket,
			final NotificationSender notificationSender, @Value("${s3.bucket.reports.path}") final String s3ReportsPath,
			@Value("${report.batch_size}") final int batchSize,
			final @Qualifier(value = "FeeScheduleReportJob") Job feeScheduleReportJob,
			final @Qualifier(value = "MonthlyARReportJob") Job monthlyArReportJob,
			final @Qualifier(value = "InvoiceReportJob") Job invoiceReportJob,
			final @Qualifier(value = "InvoiceItemReportJob") Job invoiceItemReportJob,
			final @Qualifier(value = "WriteOffReportJob") Job writeOffReportJob,
			final @Qualifier(value = "ClinicPaymentReportJob") Job clinicPaymentReportJob,
			final @Qualifier(value = "MonthlyASPReportJob") Job monthlyASPReportJob,
			final @Qualifier(value = "ClientReportJob") Job clientReportJob,
			final @Qualifier(value = "TopUpRequestReportJob") Job topUpRequestReportJob,
			final @Qualifier(value = "WalletReportJob") Job walletReportJob,
			final @Qualifier(value = "AdjustmentWriteOffReportJob") Job adjustmentWriteOffReportJob,
			final JobLauncher jobLauncher) {
		super();
		this.doctrixClientARReportFactory = doctrixClientARReportFactory;
		this.doctrixPaymentReportFactory = doctrixPaymentReportFactory;
		this.downloadReportEventService = downloadReportEventService;
		this.clientService = clientService;
		this.notificationSender = notificationSender;
		this.s3ReportsPath = s3ReportsPath;
		this.feeScheduleReportJob = feeScheduleReportJob;
		this.monthlyArReportJob = monthlyArReportJob;
		this.invoiceReportJob = invoiceReportJob;
		this.invoiceItemReportJob = invoiceItemReportJob;
		this.writeOffReportJob = writeOffReportJob;
		this.clinicPaymentReportJob = clinicPaymentReportJob;
		this.clientReportJob = clientReportJob;
		this.topUpRequestReportJob = topUpRequestReportJob;
		this.walletReportJob = walletReportJob;
		this.monthlyASPReportJob = monthlyASPReportJob;
		this.adjustmentWriteOffReportJob = adjustmentWriteOffReportJob;
		this.jobLauncher = jobLauncher;
		this.s3Wrapper = new S3Wrapper(amazonS3, bucket);
		this.batchSize = batchSize;
	}

	@Override
	@Async
	public void generateMonthlyInvoiceReport(LocalDate from, LocalDate to, boolean useInvoiceDate, long clientId,
			DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildInvoiceReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl, from, to,
					useInvoiceDate, clientId);
			jobLauncher.run(invoiceReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Validate Invoice Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateMonthlyInvoiceItemReport(LocalDate from, LocalDate to, boolean useInvoiceDate, long clientId,
			DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildInvoiceReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl, from, to,
					useInvoiceDate, clientId);
			jobLauncher.run(invoiceItemReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Validate Invoice Item Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateMonthlyARReport(LocalDate to, DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();

		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			jobParametersBuilder.addString(CommonConstants.TO_DATE_JOB_PARAM_NAME,
					to.format(DateTimeFormatter.ofPattern(CommonConstants.DATE_FORMAT_WITH_SLASH_SEP)));
			jobLauncher.run(monthlyArReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Monthly AR Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateMonthlyWriteOffReport(LocalDate from, LocalDate to, DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			buildToDateAndFromDateJobParams(jobParametersBuilder, from, to);
			jobLauncher.run(writeOffReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Write off Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateMonthlyClinicPaymentReport(LocalDate from, LocalDate to,
			DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			buildToDateAndFromDateJobParams(jobParametersBuilder, from, to);
			jobLauncher.run(clinicPaymentReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Clinic payment Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateFeeScheduleReport(DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			jobLauncher.run(feeScheduleReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Fee Schedule Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateClientReport(DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			jobLauncher.run(clientReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Client Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateDoctrixClientARReport(LocalDate from, LocalDate to, DownloadReportEvent downloadReportEvent) {
		generateReport(this.doctrixClientARReportFactory, from, to, null, downloadReportEvent);
	}

	@Override
	@Async
	public void generateDoctrixPaymentReport(LocalDate from, LocalDate to, DownloadReportEvent downloadReportEvent) {
		generateReport(this.doctrixPaymentReportFactory, from, to, null, downloadReportEvent);
	}

	@Override
	@Async
	public void generateClientWalletReport(DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			jobLauncher.run(walletReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Wallet Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateTopUpRequestReport(LocalDate from, LocalDate to, DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			buildToDateAndFromDateJobParams(jobParametersBuilder, from, to);
			jobLauncher.run(topUpRequestReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Top Up Request Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateASPReport(LocalDate from, LocalDate to, ASPReportType type,
			DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			buildToDateAndFromDateJobParams(jobParametersBuilder, from, to);
			jobParametersBuilder.addString(CommonConstants.ASP_REPORT_TYPE_JOB_PARAM_NAME, type.toString());
			jobLauncher.run(monthlyASPReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Monthly ASP Report Job : " + e.getMessage());
		}
	}

	@Override
	@Async
	public void generateAdjustmentWriteOffReport(LocalDate from, LocalDate to,
			DownloadReportEvent downloadReportEvent) {
		long currentTimeMillis = System.currentTimeMillis();
		try {
			String outputFileUrl = FileUtils.createTempFile(currentTimeMillis + "_out", ".csv").getAbsolutePath();
			JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
			buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
			buildToDateAndFromDateJobParams(jobParametersBuilder, from, to);
			jobLauncher.run(adjustmentWriteOffReportJob, jobParametersBuilder.toJobParameters()).getExitStatus();
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException | IOException e) {
			log.error("Failed to execute Adjustment Write off Report Job : " + e.getMessage());
		}
	}

	@Override
	public DownloadReportEvent createDownloadReportEvent(String filename, String owner) {
		return this.downloadReportEventService
				.saveDownloadReportEvent(DownloadReportEvent.builder().fileName(filename).owner(owner).build());
	}

	@Override
	public DownloadReportEvent createDownloadReportEventForInvoiceReport(String owner, long clientId, LocalDate from,
			LocalDate to, boolean useInvoiceDate) {
		String fileName = FileNameUtils.createMonthlyInvoiceReportFileName(from, to, useInvoiceDate);
		if (clientId > 0) {
			ClientOutputDTO client = this.clientService.findClientById(clientId);
			fileName = FileNameUtils.createMonthlyInvoiceReportFileNameWithClientName(from, to, useInvoiceDate,
					client.getName());
		}
		return this.createDownloadReportEvent(fileName, owner);
	}

	private void generateReport(AbstractMonthlyReportFactory factory, LocalDate from, LocalDate to,
			Map<String, Object> properties, DownloadReportEvent downloadReportEvent) {
		byte[] data = factory.generate(from, to, properties);
		String s3key = uploadReportToS3(data);
		this.notifyUser(s3key, downloadReportEvent);
	}

	@Override
	public void notifyUser(String fileUrl, DownloadReportEvent downloadReportEvent) {
		this.downloadReportEventService.updateDownloadReportEvent(downloadReportEvent.getId(), fileUrl);

		// send notification to user
		this.notificationSender.send(Notification.builder().title("Reports")
				.targetUsers(Set.of(downloadReportEvent.getOwner())).topics(Set.of(Topic.REPORT_GENERATION))
				.message(downloadReportEvent.getFileName() + " is generated").build());
	}

	@Override
	public String uploadResults(String fileUrl, String fileName) {
		String s3key = fileName + FILE_EXTENSION_CSV;
		s3Wrapper.upload(new File(fileUrl), s3ReportsPath + s3key);
		return s3key;
	}

	private String uploadReportToS3(byte[] data) {
		try {
			String fileName = UUID.randomUUID().toString();
			final File outputFile = FileUtils.createTempFile(fileName, TEMPLATE_EXTENSION, data);
			String s3key = fileName + TEMPLATE_EXTENSION;

			s3Wrapper.upload(outputFile, s3ReportsPath + s3key);

			return s3key;
		} catch (Exception e) {
			log.error("Error occurred while uploading report to S3");
			throw new GenerateReportException("Error occurred while uploading report to S3");
		}
	}

	JobParametersBuilder buildInvoiceReportJobParams(JobParametersBuilder jobParametersBuilder,
			DownloadReportEvent downloadReportEvent, String outputFileUrl, LocalDate from, LocalDate to,
			Boolean useInvoiceDate, long clientId) {
		buildBasicReportJobParams(jobParametersBuilder, downloadReportEvent, outputFileUrl);
		buildToDateAndFromDateJobParams(jobParametersBuilder, from, to);
		jobParametersBuilder.addString(CommonConstants.USE_INVOICE_DATE_JOB_PARAM_NAME, String.valueOf(useInvoiceDate));
		jobParametersBuilder.addLong(CommonConstants.CLIENT_ID_JOB_PARAM_NAME, clientId);
		return jobParametersBuilder;
	}

	JobParametersBuilder buildBasicReportJobParams(JobParametersBuilder jobParametersBuilder,
			DownloadReportEvent downloadReportEvent, String outputFileUrl) {
		return jobParametersBuilder.addString(CommonConstants.OUTPUT_CSV_PATH_JOB_PARAM_NAME, outputFileUrl)
				.addString(CommonConstants.JOB_TYPE_JOB_PARAM_NAME, CommonConstants.JOB_TYPE_DOWNLOAD_MONTHLY_REPORT)
				.addLong(CommonConstants.EVENT_ID_JOB_PARAM_NAME, downloadReportEvent.getId())
				.addLong(CommonConstants.BATCH_SIZE_JOB_PARAM_NAME, (long) batchSize);
	}

	JobParametersBuilder buildToDateAndFromDateJobParams(JobParametersBuilder jobParametersBuilder, LocalDate from,
			LocalDate to) {
		return jobParametersBuilder
				.addString(CommonConstants.FROM_DATE_JOB_PARAM_NAME,
						from.format(DateTimeFormatter.ofPattern(CommonConstants.DATE_FORMAT_WITH_SLASH_SEP)))
				.addString(CommonConstants.TO_DATE_JOB_PARAM_NAME,
						to.format(DateTimeFormatter.ofPattern(CommonConstants.DATE_FORMAT_WITH_SLASH_SEP)));
	}

}
