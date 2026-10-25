package ua.kpi.comsys.streams;

import java.time.Instant;

/**
 * Одне вимірювання телеметрії IoT-пристрою (незмінний запис даних).
 *
 * @param deviceId   ідентифікатор пристрою
 * @param metricType тип метрики (наприклад, "temperature")
 * @param value      значення вимірювання
 * @param timestamp  момент вимірювання
 */
public record SensorMetric(String deviceId, String metricType, double value, Instant timestamp) {
}
