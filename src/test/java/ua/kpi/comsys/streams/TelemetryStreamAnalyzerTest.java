package ua.kpi.comsys.streams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class TelemetryStreamAnalyzerTest {

    private static final double DELTA = 1e-9;
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    private final TelemetryStreamAnalyzer analyzer = new TelemetryStreamAnalyzer();

    // ---------------- допоміжні методи ----------------

    private static SensorMetric metric(String device, String type, double value) {
        return new SensorMetric(device, type, value, T0);
    }

    /** Нескінченний потік: значення 0, 1, 2, 3, ... для одного пристрою й типу. */
    private static Stream<SensorMetric> infiniteRising(String device, String type) {
        return Stream.iterate(0, i -> i + 1)
                .map(i -> new SensorMetric(device, type, i, T0.plusSeconds(i)));
    }

    /** Нескінченний потік зі сталим значенням. */
    private static Stream<SensorMetric> infiniteConstant(String device, String type, double value) {
        return Stream.generate(() -> new SensorMetric(device, type, value, T0));
    }

    // findFirstAnomaly
    @Nested
    @DisplayName("findFirstAnomaly")
    class FindFirstAnomaly {

        @Test
        @DisplayName("Скінченний потік: знаходить першу аномалію потрібного пристрою")
        void findsFirstInFiniteStream() {
            Stream<SensorMetric> stream = Stream.of(
                    metric("d1", "temp", 10),
                    metric("d2", "temp", 99),   // інший пристрій
                    metric("d1", "temp", 50),   // перша аномалія d1
                    metric("d1", "temp", 70));

            Optional<SensorMetric> result = analyzer.findFirstAnomaly(stream, "d1", 30);

            assertTrue(result.isPresent());
            assertEquals(50, result.get().value(), DELTA);
        }

        @Test
        @DisplayName("Скінченний потік: аномалії немає -> Optional.empty()")
        void emptyWhenNoAnomaly() {
            Stream<SensorMetric> stream = Stream.of(metric("d1", "temp", 1), metric("d1", "temp", 2));
            assertTrue(analyzer.findFirstAnomaly(stream, "d1", 100).isEmpty());
        }

        @Test
        @DisplayName("Порожній потік -> Optional.empty()")
        void emptyForEmptyStream() {
            assertTrue(analyzer.findFirstAnomaly(Stream.empty(), "d1", 0).isEmpty());
        }

        @Test
        @DisplayName("Значення рівне порогу не є аномалією (строго >)")
        void valueEqualToThresholdIsNotAnomaly() {
            Stream<SensorMetric> stream = Stream.of(metric("d1", "temp", 30), metric("d1", "temp", 30.0001));
            Optional<SensorMetric> result = analyzer.findFirstAnomaly(stream, "d1", 30);
            assertEquals(30.0001, result.orElseThrow().value(), DELTA);
        }

        @Test
        @DisplayName("Пристрій з іншим id ігнорується навіть із критичним значенням")
        void otherDeviceIgnored() {
            Stream<SensorMetric> stream = Stream.of(metric("d2", "temp", 1000));
            assertTrue(analyzer.findFirstAnomaly(stream, "d1", 10).isEmpty());
        }

        @Test
        @DisplayName("null-елементи в потоці пропускаються")
        void nullElementsAreSkipped() {
            Stream<SensorMetric> stream = Stream.of(null, metric("d1", "temp", 50));
            assertTrue(analyzer.findFirstAnomaly(stream, "d1", 10).isPresent());
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік (iterate): аномалія знаходиться без зависання")
        void worksOnInfiniteIterate() {
            Optional<SensorMetric> result = analyzer.findFirstAnomaly(infiniteRising("d1", "temp"), "d1", 1000);
            assertEquals(1001, result.orElseThrow().value(), DELTA);
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік (generate): перший елемент вже аномальний")
        void worksOnInfiniteGenerate() {
            Optional<SensorMetric> result = analyzer.findFirstAnomaly(infiniteConstant("d1", "temp", 500), "d1", 100);
            assertTrue(result.isPresent());
        }

        @Test
        @Timeout(2)
        @DisplayName("Ледачість: після знахідки з джерела більше не читаються елементи")
        void stopsConsumingAfterMatch() {
            AtomicInteger consumed = new AtomicInteger();
            Stream<SensorMetric> stream = infiniteRising("d1", "temp").peek(m -> consumed.incrementAndGet());

            analyzer.findFirstAnomaly(stream, "d1", 10); // значення 11 — це 12-й елемент

            assertEquals(12, consumed.get());
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік змішаних пристроїв: знаходить перший потрібний")
        void infiniteMixedDevices() {
            Stream<SensorMetric> stream = Stream.iterate(0, i -> i + 1)
                    .map(i -> new SensorMetric(i % 2 == 0 ? "even" : "odd", "temp", i, T0));
            // "odd" та value > 20 -> перше таке i = 21
            assertEquals(21, analyzer.findFirstAnomaly(stream, "odd", 20).orElseThrow().value(), DELTA);
        }

        @Test
        @DisplayName("null stream -> IllegalArgumentException")
        void nullStream() {
            assertThrows(IllegalArgumentException.class, () -> analyzer.findFirstAnomaly(null, "d1", 1));
        }

        @Test
        @DisplayName("null targetDeviceId -> IllegalArgumentException")
        void nullDeviceId() {
            assertThrows(IllegalArgumentException.class, () -> analyzer.findFirstAnomaly(Stream.empty(), null, 1));
        }
    }

    // streamActiveMetrics
    @Nested
    @DisplayName("streamActiveMetrics")
    class StreamActiveMetrics {

        @Test
        @DisplayName("Пропускає стабільний початок і бере наступні")
        void skipsStablePrefix() {
            Stream<SensorMetric> stream = Stream.of(
                    metric("d", "t", 1),
                    metric("d", "t", 2),
                    metric("d", "t", 5),
                    metric("d", "t", 6));

            List<SensorMetric> result = analyzer.streamActiveMetrics(stream, 3, 10);

            assertEquals(List.of(5.0, 6.0), result.stream().map(SensorMetric::value).toList());
        }

        @Test
        @DisplayName("Значення, рівне порогу, теж вважається стабільним (<=)")
        void valueEqualToThresholdIsSkipped() {
            Stream<SensorMetric> stream = Stream.of(metric("d", "t", 3), metric("d", "t", 4));
            List<SensorMetric> result = analyzer.streamActiveMetrics(stream, 3, 10);
            assertEquals(1, result.size());
            assertEquals(4, result.get(0).value(), DELTA);
        }

        @Test
        @DisplayName("dropWhile прибирає лише ПОЧАТОК: нижчі значення пізніше зберігаються")
        void onlyPrefixIsDropped() {
            Stream<SensorMetric> stream = Stream.of(
                    metric("d", "t", 1),
                    metric("d", "t", 9),
                    metric("d", "t", 1),   // нижче порогу, але вже не на початку
                    metric("d", "t", 8));

            List<SensorMetric> result = analyzer.streamActiveMetrics(stream, 5, 10);

            assertEquals(List.of(9.0, 1.0, 8.0), result.stream().map(SensorMetric::value).toList());
        }

        @Test
        @DisplayName("Обмеження maxCount застосовується")
        void respectsMaxCount() {
            Stream<SensorMetric> stream = Stream.of(
                    metric("d", "t", 10), metric("d", "t", 11), metric("d", "t", 12), metric("d", "t", 13));
            assertEquals(2, analyzer.streamActiveMetrics(stream, 0, 2).size());
        }

        @Test
        @DisplayName("Усі значення стабільні (скінченний потік) -> порожній список")
        void allStableGivesEmpty() {
            Stream<SensorMetric> stream = Stream.of(metric("d", "t", 1), metric("d", "t", 2));
            assertTrue(analyzer.streamActiveMetrics(stream, 10, 5).isEmpty());
        }

        @Test
        @DisplayName("Порожній потік -> порожній список")
        void emptyStream() {
            assertTrue(analyzer.streamActiveMetrics(Stream.empty(), 0, 5).isEmpty());
        }

        @Test
        @DisplayName("maxCount = 0 -> порожній список")
        void zeroMaxCount() {
            assertTrue(analyzer.streamActiveMetrics(Stream.of(metric("d", "t", 100)), 0, 0).isEmpty());
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік (iterate): пропуск 0..10, потім перші 5")
        void worksOnInfiniteIterate() {
            List<SensorMetric> result = analyzer.streamActiveMetrics(infiniteRising("d", "t"), 10, 5);

            assertEquals(List.of(11.0, 12.0, 13.0, 14.0, 15.0),
                    result.stream().map(SensorMetric::value).toList());
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік (generate): сталі активні значення, береться рівно maxCount")
        void worksOnInfiniteGenerate() {
            List<SensorMetric> result = analyzer.streamActiveMetrics(infiniteConstant("d", "t", 99), 10, 7);
            assertEquals(7, result.size());
        }

        @Test
        @Timeout(2)
        @DisplayName("Ледачість: з джерела читається лише потрібна кількість елементів")
        void consumesOnlyNeeded() {
            AtomicInteger consumed = new AtomicInteger();
            Stream<SensorMetric> stream = infiniteRising("d", "t").peek(m -> consumed.incrementAndGet());

            analyzer.streamActiveMetrics(stream, 4, 3); // пропустили 0..4 (5 шт.) + взяли 5,6,7 (3 шт.)

            assertTrue(consumed.get() <= 9, "Прочитано забагато елементів: " + consumed.get());
        }

        @Test
        @DisplayName("null stream -> IllegalArgumentException")
        void nullStream() {
            assertThrows(IllegalArgumentException.class, () -> analyzer.streamActiveMetrics(null, 1, 1));
        }

        @Test
        @DisplayName("Від'ємний maxCount -> IllegalArgumentException")
        void negativeMaxCount() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.streamActiveMetrics(Stream.empty(), 1, -1));
        }
    }

    // calculateMovingAverages + Gatherer
    @Nested
    @DisplayName("calculateMovingAverages")
    class CalculateMovingAverages {

        private void assertAverages(List<Double> expected, List<Double> actual) {
            assertEquals(expected.size(), actual.size(), "Різна кількість результатів: " + actual);
            for (int i = 0; i < expected.size(); i++) {
                assertEquals(expected.get(i), actual.get(i), DELTA, "індекс " + i);
            }
        }

        @Test
        @DisplayName("Скінченний потік: ковзне середнє з вікном 3")
        void movingAverageWindow3() {
            Stream<SensorMetric> stream = Stream.of(1, 2, 3, 4, 5).map(v -> metric("d", "temp", v));

            List<Double> result = analyzer.calculateMovingAverages(stream, "temp", 3, 100);

            // 1 | (1+2)/2 | (1+2+3)/3 | (2+3+4)/3 | (3+4+5)/3
            assertAverages(List.of(1.0, 1.5, 2.0, 3.0, 4.0), result);
        }

        @Test
        @DisplayName("Вікно 1: середнє дорівнює самому значенню")
        void windowOne() {
            Stream<SensorMetric> stream = Stream.of(5, 7, 9).map(v -> metric("d", "temp", v));
            assertAverages(List.of(5.0, 7.0, 9.0), analyzer.calculateMovingAverages(stream, "temp", 1, 10));
        }

        @Test
        @DisplayName("Вікно більше за кількість елементів: усереднюються всі наявні")
        void windowLargerThanStream() {
            Stream<SensorMetric> stream = Stream.of(2, 4).map(v -> metric("d", "temp", v));
            assertAverages(List.of(2.0, 3.0), analyzer.calculateMovingAverages(stream, "temp", 10, 10));
        }

        @Test
        @DisplayName("Метрики іншого типу ігноруються й не потрапляють у вікно")
        void otherMetricTypesIgnored() {
            Stream<SensorMetric> stream = Stream.of(
                    metric("d", "temp", 10),
                    metric("d", "humidity", 1000),
                    metric("d", "temp", 20),
                    metric("d", "humidity", 2000),
                    metric("d", "temp", 30));

            List<Double> result = analyzer.calculateMovingAverages(stream, "temp", 2, 10);

            assertAverages(List.of(10.0, 15.0, 25.0), result);
        }

        @Test
        @DisplayName("Немає метрик потрібного типу -> порожній список (скінченний потік)")
        void noMatchingType() {
            Stream<SensorMetric> stream = Stream.of(metric("d", "humidity", 5));
            assertTrue(analyzer.calculateMovingAverages(stream, "temp", 3, 10).isEmpty());
        }

        @Test
        @DisplayName("Порожній потік -> порожній список")
        void emptyStream() {
            assertTrue(analyzer.calculateMovingAverages(Stream.empty(), "temp", 3, 10).isEmpty());
        }

        @Test
        @DisplayName("outputLimit обмежує кількість результатів")
        void outputLimitApplies() {
            Stream<SensorMetric> stream = Stream.of(1, 2, 3, 4, 5).map(v -> metric("d", "temp", v));
            assertEquals(2, analyzer.calculateMovingAverages(stream, "temp", 3, 2).size());
        }

        @Test
        @DisplayName("outputLimit = 0 -> порожній список")
        void zeroOutputLimit() {
            Stream<SensorMetric> stream = Stream.of(metric("d", "temp", 1));
            assertTrue(analyzer.calculateMovingAverages(stream, "temp", 3, 0).isEmpty());
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік (iterate): перші 5 ковзних середніх")
        void worksOnInfiniteIterate() {
            List<Double> result = analyzer.calculateMovingAverages(infiniteRising("d", "temp"), "temp", 3, 5);

            // значення 0,1,2,3,4: 0 | 0.5 | 1 | 2 | 3
            assertAverages(List.of(0.0, 0.5, 1.0, 2.0, 3.0), result);
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік (generate): стале значення -> стале середнє")
        void worksOnInfiniteGenerate() {
            List<Double> result = analyzer.calculateMovingAverages(infiniteConstant("d", "temp", 42), "temp", 4, 10);

            assertEquals(10, result.size());
            result.forEach(avg -> assertEquals(42.0, avg, DELTA));
        }

        @Test
        @Timeout(2)
        @DisplayName("Нескінченний потік з чергуванням типів: береться тільки потрібний")
        void infiniteInterleavedTypes() {
            Stream<SensorMetric> stream = Stream.iterate(0, i -> i + 1)
                    .map(i -> new SensorMetric("d", i % 2 == 0 ? "temp" : "humidity", i, T0));

            // temp: 0, 2, 4, 6 ... -> вікно 2: 0 | 1 | 3 | 5
            assertAverages(List.of(0.0, 1.0, 3.0, 5.0),
                    analyzer.calculateMovingAverages(stream, "temp", 2, 4));
        }

        @Test
        @Timeout(2)
        @DisplayName("Ледачість: з нескінченного джерела читається лише потрібна кількість")
        void consumesOnlyNeeded() {
            AtomicInteger consumed = new AtomicInteger();
            Stream<SensorMetric> stream = infiniteRising("d", "temp").peek(m -> consumed.incrementAndGet());

            analyzer.calculateMovingAverages(stream, "temp", 3, 6);

            assertTrue(consumed.get() <= 7, "Прочитано забагато елементів: " + consumed.get());
        }

        @Test
        @DisplayName("Від'ємні значення та дробові середні")
        void negativeAndFractionalValues() {
            Stream<SensorMetric> stream = Stream.of(-1.5, 2.5, -4.0).map(v -> metric("d", "temp", v));
            assertAverages(List.of(-1.5, 0.5, -0.75), analyzer.calculateMovingAverages(stream, "temp", 2, 10));
        }

        @Test
        @DisplayName("null-елементи в потоці пропускаються")
        void nullElementsSkipped() {
            Stream<SensorMetric> stream = Stream.of(metric("d", "temp", 4), null, metric("d", "temp", 6));
            assertAverages(List.of(4.0, 5.0), analyzer.calculateMovingAverages(stream, "temp", 2, 10));
        }

        // ---------- валідація ----------

        @Test
        @DisplayName("null stream -> IllegalArgumentException")
        void nullStream() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(null, "temp", 3, 5));
        }

        @Test
        @DisplayName("null metricType -> IllegalArgumentException")
        void nullMetricType() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), null, 3, 5));
        }

        @Test
        @DisplayName("windowSize <= 0 -> IllegalArgumentException")
        void invalidWindowSize() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), "temp", 0, 5));
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), "temp", -2, 5));
        }

        @Test
        @DisplayName("Від'ємний outputLimit -> IllegalArgumentException")
        void invalidOutputLimit() {
            assertThrows(IllegalArgumentException.class,
                    () -> analyzer.calculateMovingAverages(Stream.empty(), "temp", 3, -1));
        }
    }

    // Кастомний Gatherer напряму
    @Nested
    @DisplayName("movingAverage Gatherer")
    class GathererTests {

        @Test
        @DisplayName("Gatherer працює як проміжна операція stream.gather(...)")
        void worksAsIntermediateOperation() {
            List<Double> result = Stream.of(2, 4, 6)
                    .map(v -> metric("d", "temp", v))
                    .gather(TelemetryStreamAnalyzer.movingAverage("temp", 2))
                    .toList();

            assertEquals(List.of(2.0, 3.0, 5.0), result);
        }

        @Test
        @DisplayName("Кожен потік отримує власний стан (повторне використання gatherer)")
        void stateIsNotSharedBetweenStreams() {
            var gatherer = TelemetryStreamAnalyzer.movingAverage("temp", 3);

            List<Double> first = Stream.of(10, 20).map(v -> metric("d", "temp", v)).gather(gatherer).toList();
            List<Double> second = Stream.of(100).map(v -> metric("d", "temp", v)).gather(gatherer).toList();

            assertEquals(List.of(10.0, 15.0), first);
            assertEquals(List.of(100.0), second); // не 10/20 з минулого потоку
        }

        @Test
        @Timeout(2)
        @DisplayName("Gatherer + limit на нескінченному потоці завершується")
        void shortCircuitsWithLimit() {
            List<Double> result = infiniteRising("d", "temp")
                    .gather(TelemetryStreamAnalyzer.movingAverage("temp", 2))
                    .limit(3)
                    .toList();

            assertEquals(List.of(0.0, 0.5, 1.5), result);
        }

        @Test
        @DisplayName("Некоректні параметри створення gatherer -> IllegalArgumentException")
        void invalidFactoryArguments() {
            assertThrows(IllegalArgumentException.class, () -> TelemetryStreamAnalyzer.movingAverage(null, 2));
            assertThrows(IllegalArgumentException.class, () -> TelemetryStreamAnalyzer.movingAverage("t", 0));
        }
    }

    // Record
    @Test
    @DisplayName("SensorMetric: record зберігає поля та має рівність за значенням")
    void sensorMetricRecord() {
        SensorMetric a = new SensorMetric("d1", "temp", 1.5, T0);
        SensorMetric b = new SensorMetric("d1", "temp", 1.5, T0);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals("d1", a.deviceId());
        assertEquals("temp", a.metricType());
        assertEquals(1.5, a.value(), DELTA);
        assertEquals(T0, a.timestamp());
        assertFalse(a.equals(new SensorMetric("d2", "temp", 1.5, T0)));
    }
}
