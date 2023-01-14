package com.linkedin.batch.chunk.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;

public class CustomRetryListener implements RetryListener {
	
	private static final Logger LOG = LoggerFactory.getLogger(CustomRetryListener.class);

	@Override
	public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
		if(context.getRetryCount() > 0) {
			LOG.info("Attempting retry");
		}
		
		return true;
	}

	@Override
	public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback,
			Throwable throwable) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback,
			Throwable throwable) {
		if(context.getRetryCount() > 0) {
			LOG.info("Failure occurred requiring a retry");
		}
	}

}
