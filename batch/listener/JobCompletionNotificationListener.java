package com.synergensolutions.sbsservice.common.batch.listener;

import com.synergensolutions.sbsservice.messages.notifications.model.Notification;
import com.synergensolutions.sbsservice.messages.notifications.model.Topic;
import com.synergensolutions.sbsservice.messages.notifications.sender.NotificationSender;
import com.synergensolutions.sbsservice.reports.model.DownloadReportEvent;
import com.synergensolutions.sbsservice.reports.service.DownloadReportEventService;
import com.synergensolutions.sbsservice.reports.service.MonthlyReportService;
import com.synergensolutions.sbsservice.reports.utils.CommonConstants;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

@Component
@Slf4j
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

	private final NotificationSender notificationSender;

	private final MonthlyReportService monthlyReportService;
	private final DownloadReportEventService downloadReportEventService;

	public JobCompletionNotificationListener(NotificationSender notificationSender,
			@Lazy final MonthlyReportService monthlyReportService,
			@Lazy final DownloadReportEventService downloadReportEventService) {
		super();
		this.monthlyReportService = monthlyReportService;
		this.downloadReportEventService = downloadReportEventService;
		this.notificationSender = notificationSender;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.info("============ JOB FINISHED ============");
			log.info("Time Taken : " + (jobExecution.getEndTime().getTime() - jobExecution.getStartTime().getTime())
					+ " ms");

			JobParameters jobParameters = jobExecution.getJobParameters();

			String jobTypeParam = Optional.ofNullable(jobParameters.getString(CommonConstants.JOB_TYPE_JOB_PARAM_NAME))
					.orElse("");

			if (jobTypeParam.equals(CommonConstants.JOB_TYPE_DOWNLOAD_MONTHLY_REPORT)) {
				DownloadReportEvent downloadReportEvent = downloadReportEventService
						.findById(jobParameters.getLong(CommonConstants.EVENT_ID_JOB_PARAM_NAME));
				String s3Key = monthlyReportService.uploadResults(
						jobParameters.getString(CommonConstants.OUTPUT_CSV_PATH_JOB_PARAM_NAME),
						downloadReportEvent.getFileName());
				monthlyReportService.notifyUser(s3Key, downloadReportEvent);
				return;
			}

			notificationSender.send(Notification.builder().title("Job Completion")
					.message(jobExecution.getJobInstance().getJobName() + " Job has been completed.")
					.targetUsers(Collections.emptySet()).topics(Set.of(Topic.JOB_COMPLETION)).build());

		}
	}
}
