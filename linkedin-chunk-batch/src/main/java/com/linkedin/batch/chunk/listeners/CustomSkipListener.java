package com.linkedin.batch.chunk.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;

import com.linkedin.batch.chunk.model.Order;
import com.linkedin.batch.chunk.model.TrackedOrder;

public class CustomSkipListener implements SkipListener<Order, TrackedOrder> {
	
	private static final Logger LOG = LoggerFactory.getLogger(CustomSkipListener.class);

	@Override
	public void onSkipInRead(Throwable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onSkipInWrite(TrackedOrder item, Throwable t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onSkipInProcess(Order item, Throwable t) {
		LOG.info("Skipping processing of item with id: ".concat(item.getOrderId().toString()));
	}

}
