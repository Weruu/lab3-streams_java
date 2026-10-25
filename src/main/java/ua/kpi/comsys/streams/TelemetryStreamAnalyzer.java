package ua.kpi.comsys.streams;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Gatherer;
import java.util.stream.Stream;

/**
 * Сервіс аналітики телеметрії. Усі методи безпечні для нескінченних потоків:
 * вони використовують лише ледачі операції та короткого замикання
 * ({@code findFirst}, {@code dropWhile}, {@code limit}).
 */
public class TelemetryStreamAnalyzer {

    /**
     * Знаходить першу метрику заданого пристрою зі значенням {@code value > criticalThreshold}.
     *
     * @return перша аномалія або {@link Optional#empty()}, якщо її немає (для скінченного потоку)
     * @throws IllegalArgumentException якщо {@code stream} або {@code targetDeviceId} дорівнює null
     */
    public Optional<SensorMetric> findFirstAnomaly(Stream<SensorMetric> stream,
                                                   String targetDeviceId,
                                                   double criticalThreshold) {
        requireNonNull(stream, "stream");
        requireNonNull(targetDeviceId, "targetDeviceId");

        return stream
                .filter(Objects::nonNull)
                .filter(metric -> targetDeviceId.equals(metric.deviceId()))
                .filter(metric -> metric.value() > criticalThreshold)
                .findFirst();
    }

    /**
     * Пропускає метрики на початку потоку, поки {@code value <= normalThreshold},
     * а потім бере не більше {@code maxCount} наступних подій.
     *
     * @throws IllegalArgumentException якщо {@code stream} дорівнює null або {@code maxCount < 0}
     */
    public List<SensorMetric> streamActiveMetrics(Stream<SensorMetric> stream,
                                                  double normalThreshold,
                                                  int maxCount) {
        requireNonNull(stream, "stream");
        if (maxCount < 0) {
            throw new IllegalArgumentException("maxCount must not be negative");
        }
        if (maxCount == 0) {
            return new ArrayList<>();
        }

        return stream
                .filter(Objects::nonNull)
                .dropWhile(metric -> metric.value() <= normalThreshold)
                .limit(maxCount)
                .toList();
    }

    /**
     * Обчислює ковзне середнє значень метрик заданого типу за допомогою кастомного {@link Gatherer}.
     * Для кожної нової метрики потрібного типу видається одне середнє по останніх
     * {@code windowSize} значеннях (на початку, поки значень менше за вікно, — по наявних).
     *
     * @param outputLimit максимальна кількість результатів (захист від нескінченності)
     * @throws IllegalArgumentException якщо {@code stream} або {@code metricType} дорівнює null,
     *                                  {@code windowSize <= 0} або {@code outputLimit < 0}
     */
    public List<Double> calculateMovingAverages(Stream<SensorMetric> stream,
                                                String metricType,
                                                int windowSize,
                                                int outputLimit) {
        requireNonNull(stream, "stream");
        requireNonNull(metricType, "metricType");
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be greater than 0");
        }
        if (outputLimit < 0) {
            throw new IllegalArgumentException("outputLimit must not be negative");
        }
        if (outputLimit == 0) {
            return new ArrayList<>();
        }

        return stream
                .gather(movingAverage(metricType, windowSize))
                .limit(outputLimit)
                .toList();
    }

    /**
     * Кастомний статевий (stateful) проміжний оператор: ковзне середнє.
     *
     * <p>Стан — вікно останніх {@code windowSize} значень (черга {@link ArrayDeque}).
     * Метрики іншого типу пропускаються без зміни стану й без видачі результату.
     *
     * @return gatherer, що перетворює {@code SensorMetric} на {@code Double}
     */
    public static Gatherer<SensorMetric, ?, Double> movingAverage(String metricType, int windowSize) {
        requireNonNull(metricType, "metricType");
        if (windowSize <= 0) {
            throw new IllegalArgumentException("windowSize must be greater than 0");
        }

        return Gatherer.<SensorMetric, ArrayDeque<Double>, Double>ofSequential(
                ArrayDeque::new,
                (window, metric, downstream) -> {
                    if (metric == null || !metricType.equals(metric.metricType())) {
                        return true; // пропускаємо елемент, чекаємо наступний
                    }
                    window.addLast(metric.value());
                    if (window.size() > windowSize) {
                        window.removeFirst(); // найстаріше значення виходить з вікна
                    }
                    double sum = 0.0;
                    for (double value : window) {
                        sum += value;
                    }
                    // true -> просимо наступний елемент; false -> downstream більше нічого не потребує
                    return downstream.push(sum / window.size());
                });
    }

    private static void requireNonNull(Object argument, String name) {
        if (argument == null) {
            throw new IllegalArgumentException(name + " cannot be null");
        }
    }
}
