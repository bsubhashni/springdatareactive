package com.couchbase.demo;


import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.repository.annotation.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.couchbase.config.AbstractReactiveCouchbaseConfiguration;
import org.springframework.data.couchbase.core.RxJavaCouchbaseTemplate;
import org.springframework.data.couchbase.core.mapping.Document;
import org.springframework.data.couchbase.core.query.N1qlPrimaryIndexed;
import org.springframework.data.couchbase.core.query.Query;
import org.springframework.data.couchbase.core.query.ViewIndexed;
import org.springframework.data.couchbase.repository.ReactiveCouchbaseRepository;
import org.springframework.data.couchbase.repository.config.EnableReactiveCouchbaseRepositories;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Repository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootApplication
@EnableReactiveCouchbaseRepositories(considerNestedRepositories = true)
public class SpringDataReactiveApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(SpringDataReactiveApplication.class, args);
	}

	@Autowired Bucket bucket;
	@Autowired UserRepository repository;
	@Autowired RxJavaCouchbaseTemplate template;

	User john = new User("johnd", "John", "Doe", 0);
	User dave = new User("daved", "Dave", "Doe", 0);
	Random r = new Random();

	@Override
	public void run(String... args) {
		repository.saveAll(Arrays.asList(john, dave)).subscribe();

		Flux.interval(Duration.ofSeconds(1))
				.doOnNext(x -> {
							int johnHeartRate = r.nextInt(175 - 60) + 60;
							int daveHeartRate = r.nextInt(175 - 60) + 60;
							if (johnHeartRate > 120) {
								bucket.mutateIn(john.id.toString()).counter("activeMinutes", 1)
										.execute();
							}
							if (daveHeartRate > 120) {
								bucket.mutateIn(dave.id.toString()).counter("activeMinutes", 1)
										.execute();
							}
						}

				).subscribe();
	}

	@RestController
	@RequiredArgsConstructor
	static class ActivityTrackerController {
		final @Autowired UserRepository repository;
		final @Autowired RxJavaCouchbaseTemplate template;

		@GetMapping("/leader")
		Mono<User> findLeader() {
			return repository.findTop1ByActiveMinutesGreaterThanOrderByActiveMinutesDesc(0);
		}

		@GetMapping(value = "/leaderStream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
		Flux<User> leaderStream() {
			return Flux.interval(Duration.ofSeconds(1))
					.flatMap(x -> repository.findTop1ByActiveMinutesGreaterThanOrderByActiveMinutesDesc(0));
		}

	}

	@Document
	@Data
	@AllArgsConstructor
	static class User {
		@Id
		String id;
		String firstname;
		String lastname;
		int activeMinutes;
	}

	@Repository
	@ViewIndexed(designDoc = "user")
	interface UserRepository extends ReactiveCouchbaseRepository<User, String> {
		Mono<User> findTop1ByActiveMinutesGreaterThanOrderByActiveMinutesDesc(int activeMinutes);
	}
}

@Configuration
class Config extends AbstractReactiveCouchbaseConfiguration {
	@Override
	protected List<String> getBootstrapHosts() {
		return Arrays.asList("127.0.0.1");
	}
	@Override
	protected String getBucketName() {
		return "activitytracker";
	}

	@Override
	protected String getBucketPassword() {
		return "";
	}
}