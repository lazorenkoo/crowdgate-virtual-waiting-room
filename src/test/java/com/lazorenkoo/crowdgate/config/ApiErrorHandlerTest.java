package com.lazorenkoo.crowdgate.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class ApiErrorHandlerTest {

    @Test
    void hidesUnexpectedExceptionDetailsAndQueryParameters() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ApiErrorHandler handler = new ApiErrorHandler(mapper);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/admin/gates?key=private-key"));

        handler.handle(exchange, new IllegalStateException("SQL password=private-password"))
                .block(Duration.ofSeconds(5));
        String body = exchange.getResponse().getBodyAsString().block(Duration.ofSeconds(5));

        assertThat(exchange.getResponse().getStatusCode().value()).isEqualTo(500);
        assertThat(body).doesNotContain("private-password", "private-key", "SQL");
        ApiError error = mapper.readValue(body, ApiError.class);
        assertThat(error.message()).isEqualTo("Internal server error");
        assertThat(error.path()).isEqualTo("/api/v1/admin/gates");
    }
}
