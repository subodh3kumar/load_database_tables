package workshop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ImportResource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
@ImportResource("classpath:application-config.xml")
public class MainApplication {

	public static void main(String[] args) {
		SpringApplication.run(MainApplication.class, args);
		log.info("main() method start");
	}
}
