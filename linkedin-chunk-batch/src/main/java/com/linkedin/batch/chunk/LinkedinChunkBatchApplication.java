package com.linkedin.batch.chunk;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;

import com.linkedin.batch.chunk.mappers.OrderFieldSetMapper;
import com.linkedin.batch.chunk.mappers.OrderRowMapper;
import com.linkedin.batch.chunk.model.Order;
import com.linkedin.batch.chunk.readers.SimpleItemReader;

@SpringBootApplication
@EnableBatchProcessing
public class LinkedinChunkBatchApplication {

	private static final Logger LOG = LoggerFactory.getLogger(LinkedinChunkBatchApplication.class);
	public static String[] tokens = new String[] { "order_id", "first_name", "last_name", "email", "cost", "item_id",
			"item_name", "ship_date" };
	
	private static String SQL_SELECT = "select order_id, first_name, last_name, email, cost, item_id, item_name, ship_date ";
	private static String SQL_FROM = "from SHIPPED_ORDER ";
	private static String SQL_SORT = "order by order_id";
	public static String ORDER_SQL = SQL_SELECT + SQL_FROM + SQL_SORT;
	
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
	

	@Bean
	public Step chunkBasedStep() throws Exception {
		return this.stepBuilderFactory.get("chunkBasedStep")
				.<Order,Order>chunk(10)
				.reader(itemReader())
				.writer(items -> {
					LOG.info("Received list of size: {}", items.size());
					items.forEach(System.out::println);
				}).build();
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
