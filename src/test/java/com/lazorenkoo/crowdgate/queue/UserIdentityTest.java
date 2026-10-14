package com.lazorenkoo.crowdgate.queue;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class UserIdentityTest {

    private final UserIdentity identity = new UserIdentity();

    @Test
    void createsCookieAndReusesItOnNextRequest() {
        var first = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/join"));
        UUID id = identity.resolve(first);
        var cookie = first.getResponse().getCookies().getFirst("cg_user_id");
        assertThat(cookie.getValue()).isEqualTo(id.toString());
        assertThat(cookie.isHttpOnly()).isTrue();
        assertThat(cookie.isSecure()).isFalse();
        assertThat(cookie.getSameSite()).isEqualTo("Lax");
        assertThat(cookie.getPath()).isEqualTo("/");
        assertThat(cookie.getMaxAge()).isEqualTo(Duration.ofDays(365));

        var next = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/join")
                .cookie(new HttpCookie("cg_user_id", cookie.getValue())));
        assertThat(identity.resolve(next)).isEqualTo(id);
        assertThat(next.getResponse().getCookies()).isEmpty();
    }

    @Test
    void replacesInvalidIdentitiesAndSecuresHttpsCookie() {
        for (String value : new String[]{"invalid", "1-1-1-1-1", ""}) {
            var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("https://example.test/join")
                    .cookie(new HttpCookie("cg_user_id", value)));
            UUID id = identity.resolve(exchange);
            var cookie = exchange.getResponse().getCookies().getFirst("cg_user_id");
            assertThat(cookie.getValue()).isEqualTo(id.toString()).isNotEqualTo(value);
            assertThat(cookie.isSecure()).isTrue();
        }
    }

    @Test
    void acceptsCanonicalUuidRegardlessOfLetterCase() {
        UUID id = UUID.randomUUID();
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("http://localhost/join")
                .cookie(new HttpCookie("cg_user_id", id.toString().toUpperCase())));
        assertThat(identity.resolve(exchange)).isEqualTo(id);
        assertThat(exchange.getResponse().getCookies()).isEmpty();
    }
}
