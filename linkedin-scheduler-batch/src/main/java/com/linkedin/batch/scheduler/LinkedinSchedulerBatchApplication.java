package com.linkedin.batch.scheduler;

import java.time.LocalDateTime;
import java.util.Date;

import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.quartz.QuartzJobBean;

@SpringBootApplication
@EnableBatchProcessing
@EnableScheduling
public class LinkedinSchedulerBatchApplication extends QuartzJobBean {

	private static final Logger LOG = LoggerFactory.getLogger(LinkedinSchedulerBatchApplication.class);

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public JobLauncher jobLauncher;

	@Autowired
	public JobExplorer jobExplorer;

	@Bean
	public Trigger trigger() {
		SimpleScheduleBuilder scheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(10)
				.repeatForever();

		return TriggerBuilder.newTrigger().forJob(jobDetail()).withSchedule(scheduleBuilder).build();
	}

	@Bean
	public JobDetail jobDetail() {
		return JobBuilder.newJob(LinkedinSchedulerBatchApplication.class).storeDurably().build();
	}

	@Override
	protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
		try {
			JobParameters params = new JobParametersBuilder(jobExplorer).getNextJobParameters(job()).toJobParameters();
			this.jobLauncher.run(job(), params);
		} catch (Exception e) {
			LOG.error("An exception was thrown: ", e);
		}
	}

	@Scheduled(cron = "0/30 * * * * *")
	public void runJob() throws JobExecutionAlreadyRunningException, JobRestartException,
			JobInstanceAlreadyCompleteException, JobParametersInvalidException, Exception {
		JobParametersBuilder parametersBuilder = new JobParametersBuilder();
		parametersBuilder.addDate("runTime", new Date());
		this.jobLauncher.run(job(), parametersBuilder.toJobParameters());
	}

	@Bean
	public Step step() throws Exception {
		return this.stepBuilderFactory.get("step").tasklet((contribution, chunkContext) -> {
			LOG.info("The run time is: {}", LocalDateTime.now());
			return RepeatStatus.FINISHED;
		}).build();

	}

	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job").incrementer(new RunIdIncrementer()).start(step()).build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinSchedulerBatchApplication.class, args);
	}

}
