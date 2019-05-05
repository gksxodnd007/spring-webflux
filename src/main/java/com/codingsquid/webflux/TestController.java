package com.codingsquid.webflux;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

@RestController
public class TestController {

    @GetMapping("/api/test")
    public Flux<Long> test() {
        return Flux.range(1, 10)
                .log()
                .map(TestController::block)
                .subscribeOn(Schedulers.newParallel("sub", 10));
    }

    private static long block(long value) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return value + 10;
    }
}
