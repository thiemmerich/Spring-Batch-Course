package com.linkedin.batch.chunk.processors;

import java.math.BigDecimal;

import org.springframework.batch.item.ItemProcessor;

import com.linkedin.batch.chunk.model.Order;
import com.linkedin.batch.chunk.model.TrackedOrder;

public class FreeShippingItemProcessor implements ItemProcessor<TrackedOrder, TrackedOrder> {

	@Override
	public TrackedOrder process(TrackedOrder item) throws Exception {
		item.setFreeShipping(item.getCost().compareTo(new BigDecimal(80)) == 1);
		return item.isFreeShipping() ? item : null;
	}

}
