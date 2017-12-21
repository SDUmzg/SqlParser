package com.Jcloud.SqlParser;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@SpringBootApplication
@ComponentScan
@MapperScan("com.Jcloud.SqlParser.Dao")
public class SqlParserApplication {
	public static void main(String[] args) {
		SpringApplication.run(SqlParserApplication.class, args);
	}
}
