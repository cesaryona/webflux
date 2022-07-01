package com.example.demo.controller;

import com.example.demo.model.Payment;
import com.example.demo.publishers.PaymentPublisher;
import com.example.demo.repository.InMemoryDatabase;
import com.example.demo.repository.PaymentRepository;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Arrays;

@RequiredArgsConstructor
@RestController
@RequestMapping("/payments")
@Slf4j
public class PaymentController {

    private final PaymentRepository paymentRepository;
    private final PaymentPublisher paymentPublisher;

    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<Payment> createPayment(@RequestBody NewPaymentInput input) {
        var userId = input.getUserId();
        log.info("payment to be processed {}", userId);
        return this.paymentRepository.createPayment(userId)
                .flatMap(payment -> this.paymentPublisher.onPaymentCreate(payment))
                .flatMap(payment -> {
                    var mono = Flux.interval(Duration.ofSeconds(1))
                            .doOnNext(it -> log.info("next tick {}", it))
                            .flatMap(tick -> this.paymentRepository.get(userId))
                            .filter(it -> Payment.PaymentStatus.APPROVED.equals(it.getStatus()))
                            .next();

                    return mono;
                })
                .doOnNext(next -> log.info("payment processed {}", userId))
                .timeout(Duration.ofSeconds(20))
                .retryWhen(
                        Retry.backoff(2, Duration.ofSeconds(1))
                                .doAfterRetry(retrySignal -> log.info("execution failed... retrying.., {}", retrySignal.totalRetries()))
                );
    }

    @GetMapping("/users")
    public Flux<Payment> findAllById(@RequestParam String ids) {
        var _ids = Arrays.asList(ids.split(","));
        log.info("collecting {} payments", _ids.size());

        return Flux.fromIterable(_ids)
                .flatMap(id -> this.paymentRepository.get(id));

    }

    @GetMapping("/ids")
    public Mono<String> getIds() {
        return Mono.fromCallable(() -> {
            return String.join(",", InMemoryDatabase.DATABASE.keySet());
        }).subscribeOn(Schedulers.parallel());
    }

    @Data
    public static class NewPaymentInput {
        private String userId;
    }
}
