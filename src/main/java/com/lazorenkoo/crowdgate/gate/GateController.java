package com.lazorenkoo.crowdgate.gate;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/admin/gates")
public class GateController {

    private final GateService service;

    public GateController(GateService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Gate> create(@RequestBody CreateGateRequest request) {
        return service.create(request);
    }

    @GetMapping("/{id}")
    public Mono<Gate> findById(@PathVariable UUID id) {
        return service.findById(id);
    }

    @GetMapping
    public Mono<GatePage> list(@RequestParam(defaultValue = "0") int page,
                               @RequestParam(defaultValue = "20") int size) {
        return service.list(page, size);
    }

    @PatchMapping("/{id}")
    public Mono<Gate> update(@PathVariable UUID id, @RequestBody UpdateGateRequest request) {
        return service.update(id, request);
    }

    @PostMapping("/{id}/open")
    public Mono<Gate> open(@PathVariable UUID id) {
        return service.changeStatus(id, GateStatus.OPEN);
    }

    @PostMapping("/{id}/pause")
    public Mono<Gate> pause(@PathVariable UUID id) {
        return service.changeStatus(id, GateStatus.PAUSED);
    }

    @PostMapping("/{id}/close")
    public Mono<Gate> close(@PathVariable UUID id) {
        return service.changeStatus(id, GateStatus.CLOSED);
    }
}
