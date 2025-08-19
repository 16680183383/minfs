package com.example.dataserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example.dataserver"})
public class DataServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataServerApplication.class, args);
    }

}
