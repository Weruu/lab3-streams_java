# Лабораторна робота №3 — Stream API, нескінченні потоки, Optional та Gatherers

**Дисципліна:** Технології розробки на платформі Java  
**Університет:** КПІ ім. Ігоря Сікорського  
**Студент:** Мальцева Софія, група ІП-41

## Опис проєкту

Сервіс `TelemetryStreamAnalyzer` аналізує **нескінченний** потік показників IoT-пристроїв
(`SensorMetric`), використовуючи лише ледачі операції та короткого замикання.

| Метод | Що робить | Ключові засоби |
|---|---|---|
| `findFirstAnomaly` | перша метрика заданого пристрою зі значенням `> criticalThreshold` | `filter`, `findFirst`, `Optional` |
| `streamActiveMetrics` | пропускає стабільний початок (`<= normalThreshold`), бере наступні `maxCount` | `dropWhile`, `limit` |
| `calculateMovingAverages` | ковзне середнє за останніми `windowSize` значеннями типу `metricType` | кастомний `Gatherer`, `limit` |

Кастомний `Gatherer` (`TelemetryStreamAnalyzer.movingAverage`) — статевий проміжний оператор
(`Gatherer.ofSequential`): його стан — вікно останніх значень (`ArrayDeque`).

Пакет: `ua.kpi.comsys.streams`.

## Розрахунок варіанта

`V = N mod 3`, де `N = 13`.

`V = 13 mod 3 = 1` (13 = 3 · 4 + 1) → **Варіант 1: Телеметрія IoT-пристроїв у реальному часі**.

## Домовленості щодо граничних випадків

- `null` у `stream` або в обов'язкових аргументах → `IllegalArgumentException`;
- `windowSize <= 0` → `IllegalArgumentException`;
- `maxCount < 0` або `outputLimit < 0` → `IllegalArgumentException`, значення `0` дає порожній список;
- ковзне середнє видається для кожної нової метрики потрібного типу; поки значень менше за `windowSize`, усереднюються всі наявні;
- `null`-елементи всередині потоку пропускаються.

## Тести

JUnit 5. Для кожного методу є сценарії зі **скінченними** та **нескінченними** потоками
(`Stream.iterate`, `Stream.generate`) із `@Timeout(2)`, а також перевірка ледачості
(лічильник `peek` показує, що з джерела прочитано лише потрібну кількість елементів).

## Запуск

Потрібно: **JDK 24 або новіший** (`java.util.stream.Gatherer` є стандартним з Java 24; проєкт перевірено на JDK 25).  
Gradle встановлювати не потрібно — використовується Gradle Wrapper.

```bash
./gradlew test
./gradlew build
```

Windows (PowerShell):

```powershell
.\gradlew.bat test
.\gradlew.bat build
```

Звіт про тести: `build/reports/tests/test/index.html`.

## Структура

```
.
├── build.gradle.kts
├── settings.gradle.kts
├── gradle/
│   ├── libs.versions.toml            # Version Catalog (JUnit 5)
│   └── wrapper/
├── gradlew, gradlew.bat
└── src/
    ├── main/java/ua/kpi/comsys/streams/
    │   ├── SensorMetric.java
    │   └── TelemetryStreamAnalyzer.java
    └── test/java/ua/kpi/comsys/streams/
        └── TelemetryStreamAnalyzerTest.java
```
