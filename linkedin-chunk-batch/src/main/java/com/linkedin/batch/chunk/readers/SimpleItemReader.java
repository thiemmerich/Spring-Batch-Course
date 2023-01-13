package com.linkedin.batch.chunk.readers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

public class SimpleItemReader implements ItemReader<String> {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleItemReader.class);

	private List<String> dataSet = new ArrayList<>();

	private Iterator<String> iterator;

	public SimpleItemReader() {
		this.dataSet.add("1");
		this.dataSet.add("2");
		this.dataSet.add("3");
		this.dataSet.add("4");
		this.dataSet.add("5");

		this.iterator = this.dataSet.iterator();
	}

	@Override
	public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
		return iterator.hasNext() ? iterator.next() : null;
	}

}
