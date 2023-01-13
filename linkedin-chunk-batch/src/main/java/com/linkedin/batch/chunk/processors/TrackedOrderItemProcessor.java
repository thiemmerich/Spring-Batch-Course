package com.linkedin.batch.chunk.processors;

import java.util.UUID;

import org.springframework.batch.item.ItemProcessor;

import com.linkedin.batch.chunk.model.Order;
import com.linkedin.batch.chunk.model.TrackedOrder;

public class TrackedOrderItemProcessor implements ItemProcessor<Order, TrackedOrder> {

	@Override
	public TrackedOrder process(Order item) throws Exception {
		TrackedOrder trackedOrder = new TrackedOrder(item);
		trackedOrder.setTrackingNumber(UUID.randomUUID().toString());
		return trackedOrder;
	}

}
