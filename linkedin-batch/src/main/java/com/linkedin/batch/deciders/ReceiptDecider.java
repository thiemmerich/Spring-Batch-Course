package com.linkedin.batch.deciders;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;

public class ReceiptDecider implements JobExecutionDecider {

	private static final Logger LOG = LoggerFactory.getLogger(ReceiptDecider.class);

	@Override
	public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
		// My logic for the random number from internet
		//Integer randomValue = rand75();
		//String result = randomValue > 0 ? "OK" : "NOT_OK";
		
		// Instructor logic for the 70% chance number
		String result = new Random().nextFloat() < .70f ? "OK" : "NOT_OK";
		LOG.info("Result is: " + result);
		return new FlowExecutionStatus(result);
	}

	// Random Function to that returns 0 or 1 with
	// equal probability
	private int rand50() {
		// rand() function will generate odd or even
		// number with equal probability. If rand()
		// generates odd number, the function will
		// return 1 else it will return 0.
		return (int) (10 * Math.random()) & 1;
	}

	// Random Function to that returns 1 with 75%
	// probability and 0 with 25% probability using
	// Bitwise AND
	private int rand75() {
		return (rand50() & rand50()) ^ 1;
	}

}
