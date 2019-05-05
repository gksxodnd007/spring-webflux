package com.codingsquid.webflux.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

class FluxTest {

    @DisplayName("Flux 생성 및 출력 테스트")
    @Test
    void createFluxAndPrint() {
        Flux<String> flux = Flux.just("hubert.squid", "coo.find", "dennis.msaa")
                .log()
                .map(String::toUpperCase)
                .subscribeOn(Schedulers.parallel());

        flux.subscribe(System.out::println);
    }

    @DisplayName("Flux flatMap 테스트")
    @Test
    void flatMap() {
        Flux.just("hubert.squid", "coo.find", "dennis.msaa", "kaka", "coding.squid")
                .log()
                .flatMap(it -> Mono.just(it.toUpperCase()).subscribeOn(Schedulers.parallel()), 5)
                .subscribe(System.out::println);
    }

    @DisplayName("Flux publishOn 테스트")
    @Test
    void publishOn() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Flux.just("hubert.squid", "coo.find", "dennis.msaa", "kaka", "coding.squid")
                .log()
                .publishOn(Schedulers.newParallel("pub"), 5)
                .map(String::toUpperCase)
                .doOnComplete(latch::countDown)
                .subscribeOn(Schedulers.newParallel("sub"))
                .subscribe(it -> System.out.println(Thread.currentThread() + it));

        latch.await(1L, TimeUnit.MINUTES);
    }

    @DisplayName("NonBlocking I/O 테스트")
    @Nested
    class NonBlockingIOTest {

        @DisplayName("NonBlocking I/O 테스트 - A")
        @Test
        void nonBlockingIOTestA() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);
            Mono.fromCallable(() -> block(1))
                    .log()
                    //이 메서드를 선언하지 않았다면 메인스레드에서 처리되기 때문에 2.5초 이상이 걸리는 작업이다.
                    .subscribeOn(Schedulers.newSingle("non-blocking"))
                    .doFinally(it -> latch.countDown())
                    .subscribe(System.out::println);

            System.out.println("-------------progress--------------");
            latch.await(1L, TimeUnit.MINUTES);
        }

        @DisplayName("NonBlocking I/O 테스트 - B")
        @Test
        void nonBlockingIOTestB() throws InterruptedException {
            ExecutorService pool = Executors.newFixedThreadPool(20);
            CountDownLatch latch = new CountDownLatch(1);

            Flux.range(1, 10)
                    .log()
                    .publishOn(Schedulers.fromExecutor(pool), 5)
                    .flatMap(it -> Mono.just(FluxTest.block(it)))
                    .subscribeOn(Schedulers.fromExecutor(pool))
                    .doOnComplete(() -> {
                        latch.countDown();
                        pool.shutdown();
                    })
                    .subscribe(it -> System.out.println(Thread.currentThread() + it.toString()));

            System.out.println("-------------progress--------------");
            latch.await(1L, TimeUnit.MINUTES);
        }

        @DisplayName("NonBlocking I/O 테스트 - C")
        @Test
        void nonBlockingIOTestC() throws InterruptedException {
            ExecutorService pool = Executors.newFixedThreadPool(20);
            CountDownLatch latch = new CountDownLatch(1);

            Flux.range(1, 10)
                    .log()
                    .publishOn(Schedulers.fromExecutor(pool))
                    .flatMap(it -> Mono.just(it).map(FluxTest::block))
                    .doOnComplete(latch::countDown)
                    .subscribeOn(Schedulers.elastic())
                    .subscribe(System.out::println);

            System.out.println("-------------progress--------------");

            latch.await(1L, TimeUnit.MINUTES);
        }

        @DisplayName("NonBlocking I/O 테스트 - D")
        @Test
        void nonBlockingIOTestD() throws InterruptedException {
            CountDownLatch latch = new CountDownLatch(1);

            Flux<Integer> flux = Flux.range(1, 10)
                    .log()
                    .publishOn(Schedulers.newParallel("pub", 10))
                    .flatMap(it -> Mono.just(it).log().map(FluxTest::block))
//                    .map(FluxTest::block)
                    .doOnNext(it -> System.out.println("data flow : " + it))
                    .doOnComplete(() -> {
                        latch.countDown();
                        System.out.println("on Complete");
                    });
//                    .subscribeOn(Schedulers.newParallel("sub", 10, true));

            System.out.println("-------------progress--------------");

            IntStream.range(1, 2)
                    .forEach(value -> {
                        System.out.println(Thread.currentThread());
                        flux.subscribe(it -> System.out.println(">>> " + Thread.currentThread() + " : " + it));
                    });

            latch.await(1L, TimeUnit.MINUTES);
        }
    }

    @DisplayName("Flux 병합 테스트")
    @Nested
    class MergeTest {

        @DisplayName("mergeWith 테스트")
        @Test
        void mergeWithFlux() throws InterruptedException {
            Flux<String> flux1 = Flux.just("hubert", "kaka", "dennis", "coo");
            Flux<String> flux2 = Flux.just("benny", "drake", "donald", "jammie");
            CountDownLatch latch = new CountDownLatch(1);

            flux1.mergeWith(flux2)
                    .log()
                    .filter(it -> it.startsWith("k"))
                    .doOnNext(System.out::println)
                    .doOnNext(it -> System.out.println(it.toUpperCase()))
                    .doOnComplete(latch::countDown)
                    .subscribeOn(Schedulers.newParallel("sub"))
                    .subscribe();

            latch.await(1L, TimeUnit.MINUTES);
        }

        @DisplayName("zip 테스트")
        @Test
        void zipTest() {
            Flux<String> flux1 = Flux.just("hubert", "kaka", "dennis", "coo");
            Flux<String> flux2 = Flux.just("benny", "drake", "donald", "jammie");
            Flux<String> flux3 = Flux.just("a", "b", "c", "d");

            Flux.zip(flux1, flux2, flux3)
                    .log()
                    .doOnNext(System.out::println)
                    .subscribe();
        }
    }



    private static int block(int value) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return value + 10;
    }
}