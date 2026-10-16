package com.lazorenkoo.crowdgate.token;

import com.lazorenkoo.crowdgate.queue.RedisQueue;
import com.lazorenkoo.crowdgate.queue.UserIdentity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1")
public class AdmissionTokenController {

    private final RedisQueue queue;
    private final AdmissionTokenCodec codec;
    private final UserIdentity identity;

    public AdmissionTokenController(RedisQueue queue, AdmissionTokenCodec codec, UserIdentity identity) {
        this.queue = queue;
        this.codec = codec;
        this.identity = identity;
    }

    @PostMapping("/queue/{queueToken}/admission-token")
    public Mono<AdmissionTokenResponse> issue(@PathVariable String queueToken, ServerWebExchange exchange) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        return queue.admissionClaims(queueToken, identity.resolve(exchange))
                .map(claims -> new AdmissionTokenResponse(codec.sign(claims), claims.expiresAt()));
    }

    @PostMapping("/tokens/validate")
    public Mono<AdmissionClaims> validate(@RequestBody AdmissionTokenRequest request, ServerWebExchange exchange) {
        exchange.getResponse().getHeaders().setCacheControl("no-store");
        return Mono.fromCallable(() -> codec.verify(request.admissionToken()));
    }
}
