package com.lazorenkoo.crowdgate.token;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AdmissionTokenConfiguration {

    @Bean
    AdmissionTokenCodec admissionTokenCodec(ObjectMapper mapper,
            @Value("${crowdgate.admission.signing-key}") String secret) {
        return new AdmissionTokenCodec(mapper, secret);
    }
}
