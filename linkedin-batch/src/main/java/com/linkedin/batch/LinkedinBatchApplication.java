package com.linkedin.batch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.linkedin.batch.deciders.DeliveryDecider;
import com.linkedin.batch.deciders.ReceiptDecider;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinBatchApplication {

	private static final Logger LOG = LoggerFactory.getLogger(LinkedinBatchApplication.class);

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Bean 
	public JobExecutionDecider decider() {
		return new DeliveryDecider();
	}
	
	@Bean
	public JobExecutionDecider receiptDecider() {
		return new ReceiptDecider();
	}
	
	@Bean
	public Step thankCustomerStep() {
		return this.stepBuilderFactory.get("thankCustomerStep").tasklet((stepContribution, chunkContext) -> {
			LOG.info("The package is correct, thanking the customer.");
			return RepeatStatus.FINISHED;
		}).build();
	}
	
	@Bean
	public Step refundCustomerStep() {
		return this.stepBuilderFactory.get("refundCustomerStep").tasklet((stepContribution, chunkContext) -> {
			LOG.info("There's something wrong with the package, customer asked for refund.");
			return RepeatStatus.FINISHED;
		}).build();
	}
	
	@Bean
	public Step leaveAtDoorStep() {
		return this.stepBuilderFactory.get("leaveAtDoorStep").tasklet((stepContribution, chunkContext) -> {
			LOG.info("Leaving the package at the door.");
			return RepeatStatus.FINISHED;
		}).build();
	}
	
	@Bean
	public Step storePackageStep() {
		return this.stepBuilderFactory.get("storePackageStep").tasklet((stepContribution, chunkContext) -> {
			LOG.info("Storing the package while the customer address is located.");
			return RepeatStatus.FINISHED;
		}).build();
	}

	@Bean
	public Step givePackageToCustomerStep() {
		return this.stepBuilderFactory.get("givePackageToCustomerStep").tasklet((stepContribution, chunkContext) -> {
			LOG.info("Given the package to the customer.");
			return RepeatStatus.FINISHED;
		}).build();
	}

	@Bean
	public Step driveToAddressStep() {
		
		boolean GOT_LOST = false;
		
		return this.stepBuilderFactory.get("driveToAddressStep").tasklet((stepContribution, chunkContext) -> {
			
			if(GOT_LOST) {
				throw new RuntimeException("Got lost driving to the address");
			}
			
			LOG.info("Successful arrived at the address.");
			return RepeatStatus.FINISHED;
		}).build();
	}

	@Bean
	public Step packageItemStep() {
		return this.stepBuilderFactory.get("packageItemStep").tasklet((stepContribution, chunkContext) -> {
			String item = chunkContext.getStepContext().getJobParameters().get("item").toString();
			String date = chunkContext.getStepContext().getJobParameters().get("run.date").toString();

			LOG.info("The {} has been packaged on {}.", item, date);
			return RepeatStatus.FINISHED;
		}).build();
	}

	@Bean
	public Job deliverPackageJob() {
		return this.jobBuilderFactory.get("deliverPackageJob")
				.start(packageItemStep())
				.next(driveToAddressStep())
					.on("FAILED").to(storePackageStep())
				.from(driveToAddressStep())
					.on("*").to(decider())
						.on("PRESENT").to(givePackageToCustomerStep())
							.next(receiptDecider()).on("OK").to(thankCustomerStep())
							.from(receiptDecider()).on("NOT_OK").to(refundCustomerStep())
					.from(decider())
						.on("NOT_PRESENT").to(leaveAtDoorStep())
				.end()
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinBatchApplication.class, args);
	}

}
