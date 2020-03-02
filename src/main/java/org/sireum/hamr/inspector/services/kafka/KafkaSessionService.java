package org.sireum.hamr.inspector.services.kafka;

import lombok.extern.slf4j.Slf4j;
import org.sireum.hamr.inspector.services.Session;
import org.sireum.hamr.inspector.services.SessionService;
import org.sireum.hamr.inspector.services.SessionStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class KafkaSessionService implements SessionService {

    @Override
    public Flux<Session> sessions() {
        log.error("calling dummy implementation for kafka session service!");
        return Flux.just(new Session("topic-1"));
    }

    @Override
    public Mono<Long> startTimeOf(Session session) {
        log.error("calling dummy implementation for kafka session service!");
        return Mono.just(System.currentTimeMillis() - 500_000L);
    }

    @Override
    public Mono<Long> stopTimeOf(Session session) {
        log.error("calling dummy implementation for kafka session service!");
        return Mono.empty();
    }

    @Override
    public Mono<SessionStatus> statusOf(Session session) {
        log.error("calling dummy implementation for kafka session service!");
        return Mono.just(SessionStatus.RUNNING);
    }

    @Override
    public Flux<GroupedFlux<Session, SessionStatus>> liveStatusUpdates() {
        log.error("calling dummy implementation for kafka session service!");
        return Mono.just(SessionStatus.RUNNING)
                .concatWith(Mono.delay(Duration.of(5, ChronoUnit.DAYS))
                        .then(Mono.just(SessionStatus.COMPLETED))).groupBy(it -> new Session("topic-1"));
    }
}
