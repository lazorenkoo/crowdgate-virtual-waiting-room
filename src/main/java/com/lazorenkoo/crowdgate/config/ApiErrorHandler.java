package com.lazorenkoo.crowdgate.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
@Order(-2)
public class ApiErrorHandler implements ErrorWebExceptionHandler {

    private final ObjectMapper mapper;

    public ApiErrorHandler(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable error) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(error);
        }
        HttpStatusCode status = HttpStatus.INTERNAL_SERVER_ERROR;
        String message = "Internal server error";
        if (error instanceof ResponseStatusException responseError) {
            status = responseError.getStatusCode();
            exchange.getResponse().getHeaders().putAll(responseError.getHeaders());
            if (status.is4xxClientError()) {
                HttpStatus known = HttpStatus.resolve(status.value());
                message = known == null ? "Request rejected" : known.getReasonPhrase();
                if (responseError.getClass() == ResponseStatusException.class
                        && responseError.getReason() != null) {
                    message = responseError.getReason();
                }
            }
        }
        ApiError body = new ApiError(status.value(), message, exchange.getRequest().getPath().value());
        try {
            byte[] json = mapper.writeValueAsBytes(body);
            exchange.getResponse().setStatusCode(status);
            exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
            return exchange.getResponse().writeWith(Mono.just(
                    exchange.getResponse().bufferFactory().wrap(json)));
        } catch (JsonProcessingException serializationError) {
            return Mono.error(serializationError);
        }
    }
}
