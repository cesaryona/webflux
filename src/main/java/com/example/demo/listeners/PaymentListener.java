package com.example.demo.listeners;

import com.example.demo.model.Payment;
import com.example.demo.model.PubSubMessage;
import com.example.demo.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentListener implements InitializingBean {

    private final Sinks.Many<PubSubMessage> sink;
    private final PaymentRepository paymentRepository;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.sink.asFlux()
                .delayElements(Duration.ofSeconds(2))
                .subscribe(
                        next -> {
                            log.info("on next message");
                            this.paymentRepository.processPayment(next.getKey(), Payment.PaymentStatus.APPROVED)
                                    .doOnNext(it -> log.info("payment processed on listener"))
                                    .subscribe();
                        },
                        error -> {
                            log.info("on pub-sub listener observe error", error);
                        },
                        () -> {
                            log.info("on pub-sub listener complete");
                        }
                );
    }
}
