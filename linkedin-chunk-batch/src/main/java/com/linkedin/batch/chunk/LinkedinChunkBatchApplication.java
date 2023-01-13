package com.linkedin.batch.chunk;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.support.builder.CompositeItemProcessorBuilder;
import org.springframework.batch.item.validator.BeanValidatingItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import com.linkedin.batch.chunk.mappers.OrderRowMapper;
import com.linkedin.batch.chunk.model.Order;
import com.linkedin.batch.chunk.model.TrackedOrder;
import com.linkedin.batch.chunk.processors.FreeShippingItemProcessor;
import com.linkedin.batch.chunk.processors.TrackedOrderItemProcessor;
import com.linkedin.batch.chunk.repository.OrderItemPreparedStatementSetter;


@SpringBootApplication
@EnableBatchProcessing
public class LinkedinChunkBatchApplication {

	private static final Logger LOG = LoggerFactory.getLogger(LinkedinChunkBatchApplication.class);
	public static String[] tokens = new String[] { "orderId", "firstName", "lastName", "email", "cost", "itemId",
			"itemName", "shipDate" };
	
	private static String SQL_SELECT = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date ";
	private static String SQL_FROM = "from SHIPPED_ORDER ";
	private static String SQL_SORT = "order by order_id";
	public static String ORDER_SQL = SQL_SELECT + SQL_FROM + SQL_SORT;
	
	public static String INSERT_ORDER_SQL = "insert into "
			+ "SHIPPED_ORDER_OUTPUT(order_id, first_name, last_name, email, item_id, item_name, cost, ship_date)"
			+ " values(:orderId,:firstName,:lastName,:email,:itemId,:itemName,:cost,:shipDate)";
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	
	
	// Chapter 4 EP 4 - Reading a flatfile in this case a csv file
//	@Bean
//	public ItemReader<Order> itemReader() {
//		FlatFileItemReader<Order> itemReader = new FlatFileItemReader<>();
//		itemReader.setLinesToSkip(1);
//		itemReader.setResource(new FileSystemResource("Z:\\Ambiente\\Projetos\\shipped_orders.csv"));
//		
//		DefaultLineMapper<Order> lineMapper = new DefaultLineMapper<>();
//		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
//		tokenizer.setNames(tokens);
//		
//		lineMapper.setLineTokenizer(tokenizer);
//		lineMapper.setFieldSetMapper(new OrderFieldSetMapper());
//		
//		itemReader.setLineMapper(lineMapper);
//		
//		return itemReader;
//	}
	
	// Chapter 4 EP 5 - Reading data from DB
	@Autowired
	private DataSource dataSource;
	
//	@Bean
//	public ItemReader<Order> itemReader() {
//		return new JdbcCursorItemReaderBuilder<Order>()
//				.dataSource(dataSource)
//				.name("jdbcCursorItemReader")
//				.sql(ORDER_SQL)
//				.rowMapper(new OrderRowMapper())
//				.build();
//	}
	
	// Chapter 4 EP 6 - Reading data fro DB using Multi Thread JDBC Item reader
	@Bean
	public PagingQueryProvider queryProvider() throws Exception {
		SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
		factory.setSelectClause(SQL_SELECT);
		factory.setFromClause(SQL_FROM);
		factory.setSortKey("order_id");
		factory.setDataSource(dataSource);
		
		return factory.getObject();
	}
	
	// Chapter 5 EP 2 - Flatfile item writer
//	public ItemWriter<Order> itemWriter() {
//		FlatFileItemWriter<Order> itemWriter = new FlatFileItemWriter<Order>();
//		itemWriter.setResource(new FileSystemResource("Z:\\Ambiente\\Projetos\\shipped_orders_output.csv"));
//		
//		BeanWrapperFieldExtractor<Order> fieldExtractor = new BeanWrapperFieldExtractor<>();
//		fieldExtractor.setNames(tokens);
//		
//		DelimitedLineAggregator<Order> aggregator = new DelimitedLineAggregator<>();
//		aggregator.setDelimiter(",");
//		aggregator.setFieldExtractor(fieldExtractor);
//		
//		itemWriter.setLineAggregator(aggregator);
//		return itemWriter;
//	}
	
	// Chapter 5 EP 3 - JDBC item writer
//	@Bean
//	public ItemWriter<Order> itemWriter() {
//		return new JdbcBatchItemWriterBuilder<Order>()
//				.dataSource(dataSource)
//				.sql(INSERT_ORDER_SQL)
//				.beanMapped()
//				.build();
//	}
	
	// Chapter 5 Challenge - Write the data to a JSON file
	@Bean
	public ItemWriter<TrackedOrder> itemWriter() {
		return new JsonFileItemWriterBuilder<TrackedOrder>()
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<TrackedOrder>())
                .resource(new FileSystemResource("Z:\\Ambiente\\Projetos\\shipped_orders_output.json"))
                .name("jsonFileItemWriter")
                .build();
	}
	
	@Bean
	public ItemReader<Order> itemReader() throws Exception {
		return new JdbcPagingItemReaderBuilder<Order>()
				.dataSource(dataSource)
				.name("jdbcCursorItemReader")
				.queryProvider(queryProvider())
				.rowMapper(new OrderRowMapper())
				.pageSize(10)
				.build();
	}
	
	// Chapter 6 EP 2 - Item processor
	@Bean
	public ItemProcessor<Order, Order> orderValidatingItemProcessor() {
		BeanValidatingItemProcessor<Order> itemProcessor = new BeanValidatingItemProcessor<>();
		itemProcessor.setFilter(true);
		return itemProcessor;
	}
	
	// Chapter 6 EP 3 - Transforming an item using a processor
	@Bean
	public ItemProcessor<Order, TrackedOrder> trackedOrderItemProcessor() {
		return new TrackedOrderItemProcessor();
	}
	
	// Chapter 6 EP 4 - Creating a chained processor
	@Bean
	public ItemProcessor<Order, TrackedOrder> compositeItemProcessor() {
		return new CompositeItemProcessorBuilder<Order, TrackedOrder>()
				.delegates(orderValidatingItemProcessor(), trackedOrderItemProcessor(), freeShippingItemProcessor())
				.build();
	}
	
	// Chapter 6 Challenge - Create a item processor for free shipping, but the only
	// items eligible for free shipping are the ones that costs more than $80 and
	// from government
	@Bean
	public ItemProcessor<TrackedOrder, TrackedOrder> freeShippingItemProcessor() {
		return new FreeShippingItemProcessor();
	}
	
	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order, TrackedOrder>chunk(10)
				.reader(itemReader())
				.processor(compositeItemProcessor())
				.writer(itemWriter())
				.build();
	}
	

	@Bean
	public Job job() throws Exception {
		return this.jobBuilderFactory.get("job")
				.start(chunkBasedStep())
				.build();
	}

	public static void main(String[] args) {
		SpringApplication.run(LinkedinChunkBatchApplication.class, args);
	}

}
