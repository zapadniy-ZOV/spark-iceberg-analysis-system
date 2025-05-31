package itmo.rshd.hadoop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;

// If spring-dotenv is on the classpath and a .env file exists,
// it should be picked up automatically by Spring Boot's normal property source loading.
// We don't necessarily need to manually add DotenvPropertySource if using recent spring-dotenv versions
// with Spring Boot 3.x.

@SpringBootApplication(exclude = {GsonAutoConfiguration.class})
public class ZOVHadoopa {

    public static void main(String[] args) {
        // SpringApplication.run will automatically set up the environment
        // and load properties from application.properties, application.yml, .env (if spring-dotenv is configured)
        SpringApplication.run(ZOVHadoopa.class, args);
    }
}
