package com.github.dzlog;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.ImportResource;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Created by binsong.li
 */

@ImportResource(locations = {"classpath*:server-context.xml"})
@EnableAspectJAutoProxy
@EnableScheduling
@SpringBootApplication
public class DzlogAppMain extends SpringBootServletInitializer {

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
		return application.sources(DzlogAppMain.class);
	}

	public static void main(String[] args) {
		SpringApplication.run(DzlogAppMain.class, args);
	}
}
