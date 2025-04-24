package com.temesoft.fs.spring;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.http.client.HttpClientAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@SpringBootApplication(exclude = {HttpClientAutoConfiguration.class})
@TestPropertySource(properties = {"spring.main.banner-mode=off"})
public class TestApp {
}
