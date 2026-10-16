package com.lazorenkoo.crowdgatedemo;

import com.lazorenkoo.crowdgate.token.AdmissionClaims;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@Component
public class DemoActivationClient {

    private final WebClient client;

    public DemoActivationClient(WebClient.Builder builder,
            @Value("${crowdgate.demo.crowdgate-url}") String baseUrl) {
        URI uri = URI.create(baseUrl);
        if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                || uri.getHost() == null || uri.getUserInfo() != null
                || uri.getQuery() != null || uri.getFragment() != null
                || !(uri.getPath().isEmpty() || "/".equals(uri.getPath()))) {
            throw new IllegalArgumentException("CrowdGate URL must be an HTTP origin");
        }
        this.client = builder.baseUrl(baseUrl).build();
    }

    public Mono<Void> activate(AdmissionClaims claims, String token, UUID userId) {
        String expected = claims.gateId() + ":" + claims.sessionId();
        return client.post().uri("/api/v1/tokens/redeem")
                .cookie("cg_user_id", userId.toString())
                .bodyValue(Map.of("admissionToken", token))
                .retrieve().bodyToMono(ActivationResult.class)
                .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.BAD_GATEWAY,
                        "CrowdGate returned an invalid activation")))
                .flatMap(result -> "ACTIVE".equals(result.status()) && expected.equals(result.queueToken())
                        ? Mono.<Void>empty() : Mono.error(new ResponseStatusException(HttpStatus.BAD_GATEWAY,
                                "CrowdGate returned an invalid activation")))
                .timeout(Duration.ofSeconds(3))
                .onErrorMap(WebClientResponseException.class, error -> new ResponseStatusException(
                        error.getStatusCode().is4xxClientError() ? error.getStatusCode()
                                : HttpStatus.SERVICE_UNAVAILABLE, "CrowdGate activation failed"))
                .onErrorMap(error -> error instanceof WebClientRequestException || error instanceof TimeoutException,
                        error -> new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CrowdGate is unavailable"));
    }

    public record ActivationResult(String queueToken, String status) {
    }
}
