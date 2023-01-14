package com.linkedin.batch.chunk.processors;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.linkedin.batch.chunk.exceptions.OrderProcessingException;
import com.linkedin.batch.chunk.model.Order;
import com.linkedin.batch.chunk.model.TrackedOrder;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

	private static final Logger LOG = LoggerFactory.getLogger(TrackedOrderItemProcessor.class);
	
	@Override
	public TrackedOrder process(Order item) throws Exception {
		LOG.info("Processing order with ID: {}", item.getOrderId());
		LOG.info("Processing with thread: {}", Thread.currentThread().getName());
		
		TrackedOrder trackedOrder = new TrackedOrder(item);
		trackedOrder.setTrackingNumber(this.getTrackingNumber());
		return trackedOrder;
	}

	private String getTrackingNumber() throws OrderProcessingException {
		
		if(Math.random() < .05) {
			throw new OrderProcessingException();
		}

		return UUID.randomUUID().toString(); 
	}

}
