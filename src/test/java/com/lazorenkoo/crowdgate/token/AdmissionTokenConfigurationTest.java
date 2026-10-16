package com.lazorenkoo.crowdgate.token;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdmissionTokenConfigurationTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(AdmissionTokenConfiguration.class)
            .withBean(org.springframework.context.support.PropertySourcesPlaceholderConfigurer.class,
                    org.springframework.context.support.PropertySourcesPlaceholderConfigurer::new)
            .withBean(ObjectMapper.class, () -> new ObjectMapper().findAndRegisterModules());

    @Test
    void requiresAnExplicitSigningKeyWithAtLeastThirtyTwoBytes() {
        context.run(result -> assertThat(result).hasFailed());
        context.withPropertyValues("crowdgate.admission.signing-key=short")
                .run(result -> assertThat(result).hasFailed());
        context.withPropertyValues("crowdgate.admission.signing-key=configuration-test-key-with-32-plus-bytes")
                .run(result -> {
                    assertThat(result).hasNotFailed().hasSingleBean(AdmissionTokenCodec.class);
                    AdmissionTokenCodec codec = result.getBean(AdmissionTokenCodec.class);
                    Instant now = Instant.now().minusSeconds(1);
                    AdmissionClaims claims = new AdmissionClaims(UUID.randomUUID(), UUID.randomUUID(),
                            now, now.plusSeconds(60), UUID.randomUUID());
                    assertThat(codec.verify(codec.sign(claims))).isEqualTo(claims);
                });
    }

    @Test
    void rejectsBlankKeysAndSignedNullPayloadWithSafeErrors() {
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        assertThatThrownBy(() -> new AdmissionTokenCodec(mapper, " ".repeat(32)))
                .isInstanceOf(IllegalArgumentException.class);
        AdmissionTokenCodec codec = new AdmissionTokenCodec(mapper, "configuration-test-key-with-32-plus-bytes");
        assertThatThrownBy(() -> codec.verify(codec.sign(null)))
                .isInstanceOfSatisfying(ResponseStatusException.class, error -> {
                    assertThat(error.getStatusCode().value()).isEqualTo(401);
                    assertThat(error.getReason()).isEqualTo("Invalid admission token");
                });
    }
}
