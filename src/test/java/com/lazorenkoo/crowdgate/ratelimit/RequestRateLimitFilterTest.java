package com.lazorenkoo.crowdgate.ratelimit;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RequestRateLimitFilterTest {

    private final RedisRateLimiter limiter = mock(RedisRateLimiter.class);
    private final RequestRateLimitFilter filter = new RequestRateLimitFilter(limiter, 20, 10);

    @Test
    void rejectsExcessRequestsAndFailsClosedOnlyForLimiterErrors() {
        AtomicInteger calls = new AtomicInteger();
        when(limiter.acquire(any(), any(), eq(20), eq(10))).thenReturn(Mono.just(1500L));
        var denied = exchange("/api/v1/queue/token");
        filter.filter(denied, ignored -> { calls.incrementAndGet(); return Mono.empty(); }).block();
        assertThat(denied.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(denied.getResponse().getHeaders().getFirst("Retry-After")).isEqualTo("2");
        assertThat(denied.getResponse().getHeaders().getCacheControl()).isEqualTo("no-store");
        when(limiter.acquire(any(), any(), anyInt(), anyInt())).thenReturn(Mono.error(new RuntimeException()));
        var unavailable = exchange("/api/v1/tokens/validate");
        filter.filter(unavailable, ignored -> { calls.incrementAndGet(); return Mono.empty(); }).block();
        assertThat(unavailable.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(calls).hasValue(0);
        when(limiter.acquire(any(), any(), anyInt(), anyInt())).thenReturn(Mono.just(0L));
        filter.filter(exchange("/api/v1/gates/id/join"), ignored -> {
            calls.incrementAndGet(); return Mono.empty();
        }).block();
        assertThat(calls).hasValue(1);
        var failure = new IllegalStateException("endpoint failure");
        assertThatThrownBy(() -> filter.filter(exchange("/api/v1/queue/id"), ignored -> Mono.error(failure))
                .block()).isSameAs(failure);
    }

    @Test
    void changingCookiesAndForwardedHeadersDoesNotChangeTheSocketBucket() {
        when(limiter.acquire(any(), any(), anyInt(), anyInt())).thenReturn(Mono.just(0L));
        for (int i = 0; i < 2; i++) {
            var request = MockServerHttpRequest.get("/api/v1/queue/token")
                    .remoteAddress(new InetSocketAddress("127.0.0.1", 1000 + i))
                    .cookie(new HttpCookie("cg_user_id", UUID.randomUUID().toString()))
                    .header("X-Forwarded-For", "192.0.2." + i).build();
            filter.filter(MockServerWebExchange.from(request), ignored -> Mono.empty()).block();
        }
        UUID client = UUID.nameUUIDFromBytes("127.0.0.1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        verify(limiter, times(2)).acquire(new UUID(0, 0), client, 20, 10);
        verifyNoMoreInteractions(limiter);
    }

    @Test
    void bypassesAdminAndNonApiPathsAndRejectsInvalidConfiguration() {
        for (String path : new String[]{"/api/v1/admin/gates", "/health", "/"}) {
            filter.filter(exchange(path), ignored -> Mono.empty()).block();
        }
        verifyNoInteractions(limiter);
        assertThatThrownBy(() -> new RequestRateLimitFilter(limiter, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private MockServerWebExchange exchange(String path) {
        return MockServerWebExchange.from(MockServerHttpRequest.get(path));
    }
}
