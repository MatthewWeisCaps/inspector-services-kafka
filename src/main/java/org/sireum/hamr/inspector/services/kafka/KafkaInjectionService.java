package org.sireum.hamr.inspector.services.kafka;

import lombok.extern.slf4j.Slf4j;
import org.sireum.hamr.inspector.common.Injection;
import org.sireum.hamr.inspector.services.InjectionService;
import org.sireum.hamr.inspector.services.Session;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaInjectionService implements InjectionService {

    @Override
    public void inject(Session session, Injection injection) {
        log.error("inject has not been implemented and is doing nothing despite being called!");
    }
}
