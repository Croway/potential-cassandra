package com.example.camelsql;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.processor.aggregate.cassandra.CassandraAggregationRepository;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;

@Component
public class CassandraTest extends RouteBuilder {

	public CassandraAggregationRepository cassandraAggregationRepository() throws Exception {
		InetSocketAddress endpoint
				= new InetSocketAddress("localhost", 9042);
		//create a new session
		CqlSession session = CqlSession.builder()
				.withLocalDatacenter("datacenter1")
				.withKeyspace("db")
				.addContactPoint(endpoint).build();

		final CassandraAggregationRepository repository = new CassandraAggregationRepository(session);
		repository.setTable("employee");
		repository.setPKColumns("id");
		return repository;
	}

	@Override
	public void configure() throws Exception {
		from("timer:select?repeatCount=1")
				.setHeader("aaa").header("aaa")
				.setBody(body())
				.aggregate(simple("${header.aaa} == 0"), new MyAggregationStrategy())
				.aggregationRepository(cassandraAggregationRepository())
				.completionSize(10).log("aggregated exchange id ${exchangeId} with ${body}").to("mock:aggregated")
				// simulate errors the first two times
				.process(exchange -> {
					throw new IllegalArgumentException("Damn");
				}).end();
	}
}
