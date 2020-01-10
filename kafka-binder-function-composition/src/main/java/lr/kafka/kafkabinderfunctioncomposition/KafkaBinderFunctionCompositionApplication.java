package lr.kafka.kafkabinderfunctioncomposition;

import java.util.function.Function;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaBinderFunctionCompositionApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaBinderFunctionCompositionApplication.class, args);
	}

	
	// kafka-server-start C:\Tools\kafka_2.11-2.2.1\kafka_2.11-2.2.1\config\server.properties
	// kafka-console-producer --broker-list localhost:9092 --topic uppercase-in-0 
	// kafka-console-consumer --bootstrap-server localhost:9092 --topic uppercase-out-0 --group sample_group_id

	
	@Bean
	public Function<String, String> uppercase() {
		return v -> {
			v =  v.toUpperCase();
			System.out.println("Uppercase: " + v);
			return v;
		};
	}
	
}
