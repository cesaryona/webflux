package com.example.demo.repository;

import com.example.demo.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRepository {

    private static final ThreadFactory THREAD_FACTORY = new CustomizableThreadFactory("database-");
    private static final Scheduler DB_SCHEDULER = Schedulers.fromExecutor(Executors.newFixedThreadPool(8, THREAD_FACTORY));

    private final Database database;

    public Mono<Payment> createPayment(String userId) {
        var payment = Payment.builder().id(UUID.randomUUID().toString()).userId(userId).status(Payment.PaymentStatus.PENDING).build();

        return Mono.fromCallable(() -> {
                    log.info("Saving payment transaction for user {}", userId);
                    return this.database.save(userId, payment);
                })
                .delayElement(Duration.ofMillis(20))
                .subscribeOn(DB_SCHEDULER)
                .doOnNext(next -> log.info("Payment received {}", next.getUserId()));

    }

    public Mono<Payment> get(final String userId) {
        log.info("getting payment from database - {}", userId);
        return Mono.defer(() -> {
            final Optional<Payment> payment = this.database.get(userId, Payment.class);
            return Mono.justOrEmpty(payment);
        }).delayElement(Duration.ofMillis(20)).subscribeOn(DB_SCHEDULER).doOnNext(it -> log.info("payment received - {}", userId));
    }

    public Mono<Payment> processPayment(String key, Payment.PaymentStatus status) {
        log.info("on payment {} received to status {}", key, status);
        return get(key).flatMap(payment -> Mono.fromCallable(() -> {
                    log.info("processing payment {} to status {}", key, status);
                    return this.database.save(key, payment.withStatus(status));
                }).delayElement(Duration.ofMillis(20)).subscribeOn(DB_SCHEDULER)
        );
    }
}
