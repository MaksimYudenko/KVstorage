package by.webapp.kvstorage;

import by.webapp.kvstorage.util.NodeLoader;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;

@SpringBootApplication
public class Runner {

    private static final Logger logger = LogManager.getLogger(Runner.class);

    public static void main(String[] args) {
        NodeLoader.setNode(System.getProperty("nodeName"));
        SpringApplication.run(Runner.class, Arrays.toString(args));
        logger.debug("Application started.");
    }

}