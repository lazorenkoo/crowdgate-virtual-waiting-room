package com.lazorenkoo.crowdgate.gate;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.UUID;

@Service
public class GateService {

    private final GateRepository repository;

    public GateService(GateRepository repository) {
        this.repository = repository;
    }

    public Mono<Gate> create(CreateGateRequest request) {
        return Mono.fromCallable(() -> {
            validate(request);
            Gate gate = new Gate(request.name().trim(), request.admissionRate(),
                    request.maxActiveUsers(), request.sessionTtlSeconds(), request.tokenTtlSeconds());
            return repository.saveAndFlush(gate);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Gate> findById(UUID id) {
        return Mono.fromCallable(() -> repository.findById(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Gate not found")))
                .subscribeOn(Schedulers.boundedElastic());
    }

    private void validate(CreateGateRequest request) {
        if (request.name() == null || request.name().isBlank() || request.name().trim().length() > 255) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Name must contain 1 to 255 characters");
        }
        if (request.admissionRate() <= 0 || request.maxActiveUsers() <= 0
                || request.sessionTtlSeconds() <= 0 || request.tokenTtlSeconds() <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Gate limits and TTL must be positive");
        }
    }
}
