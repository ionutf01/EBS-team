# Random Balanced Subscription and Publication Generator

This application generates random balanced sets of subscriptions and publications according to specified distributions. It ensures exact percentages of fields and operator distributions in the generated subscriptions.

## Features

- Configurable numbers of publications and subscriptions
- Deterministic field presence frequencies for subscriptions
- Configurable equality operator frequencies for specific fields
- Parallel generation with configurable number of threads
- Performance measurement and comparison

## Implementation Details

### Publication Structure

Publications have a fixed structure with the following fields:
- `stationid` (integer): Random value between 1-100
- `city` (string): Selected from a predefined set (Bucharest, Cluj, Iasi, etc.)
- `temp` (integer): Random value between -10 and 30
- `rain` (double): Random value between 0 and 50
- `wind` (integer): Random value between 0 and 100
- `direction` (string): Selected from a predefined set (N, NE, E, etc.)
- `date` (date): Random date between 2023-01-01 and 2025-12-31

### Subscription Structure

Subscriptions consist of conditions on fields, where:
- Each field can be present or absent based on configured frequency
- Each field uses an operator (=, >, <, >=, <=, !=)
- Equality operator frequency can be configured for specific fields

### Field Distribution Methodology

The implementation uses a deterministic approach to ensure precise percentage distributions:

1. **Pre-calculation**: Calculate exact counts of subscriptions that should contain each field
2. **Controlled assignment**: Randomly select which subscriptions will contain each field to match the exact target count
3. **Operator distribution**: For fields with equality operator requirements, precisely control how many use the equality operator

This approach guarantees that the actual percentages exactly match the configured targets, avoiding the variance inherent in random probability-based approaches.

### Parallelization Approach

- **Type of parallelization**: Thread-based using Java's ExecutorService
- **Parallel generation process**:
    1. For publications: Direct parallel generation with task division
    2. For subscriptions: Two-phase approach with template creation and synchronized result collection

## Performance Analysis

Tests run on Intel Core Ultra 7 155H, 16 cores, 22 threads with 16GB RAM with the following parameters:
- 10,000 publications
- 5,000 subscriptions

| Thread Count | Execution Time (ms) | Speedup |
|--------------|---------------------|---------|
| 1 (sequential)| 4520              | 1.0x    |
| 4            | 1380               | 3.3x    |
| 12           | 950                | 4.8x    |

## Configuration

The main configuration parameters in the Generator class:
- `publicationCount`: Number of publications to generate
- `subscriptionCount`: Number of subscriptions to generate
- `fieldFrequency`: Map defining percentage of subscriptions that should contain each field
- `equalityFrequency`: Map defining percentage of field occurrences that should use equality operator

## Validation Results

The application validates that the generated subscriptions match the configured distributions precisely:

```
Validation Results:
  Field 'stationid': target=30%, actual=30.0% (1500/5000)
  Field 'city': target=90%, actual=90.0% (4500/5000)
  Field 'temp': target=60%, actual=60.0% (3000/5000)
  Field 'rain': target=40%, actual=40.0% (2000/5000)
  Field 'wind': target=50%, actual=50.0% (2500/5000)
  Field 'direction': target=20%, actual=20.0% (1000/5000)
  Field 'date': target=10%, actual=10.0% (500/5000)
  Equality operator for 'city': target=70%, actual=70.0% (3150/4500)
```

## How to Run

```bash
javac Generator.java Publication.java Subscription.java
java Generator
```

The program will generate the data and output performance metrics and validation results.