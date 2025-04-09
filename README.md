# Balanced Subscription & Publication Generator

This project generates balanced sets of subscriptions and publications with support for field frequency configuration, operator distribution, and parallelized generation using threads.

## Team Members

- Feraru Ionuț MSD1
- Rotaru Oana-Dumitrița MISS1 G2
- Sîrghi Constantin-Florin MSD1

---

## Parallelization Details

- **Type of parallelization used**: Multithreading (Java ExecutorService)
- **Tested thread counts**: 1 (no parallelization), 4, 6 and 8

## Test Results

### 🧪 Test Run 1 - Feraru Ionuț
- Processor: Intel(R) Core(TM) Ultra 7 155H, 1.40 GHz

| Run # | Publications | Subscriptions | Parallelism (Processes) | Time (seconds) |
|-------|--------------|---------------|-------------------------|----------------|
| 1     | 10000        | 5000          | 1                       | 0.08           |
| 2     | 10000        | 5000          | 4                       | 0.05           |
| 3     | 10000        | 5000          | 6                       | 0.01           |
| 4     | 10000        | 5000          | 8                       | 0.02           |
| 5     | 1000         | 500           | 1                       | 0.00           |
| 6     | 1000         | 500           | 4                       | 0.01           |
| 7     | 1000         | 500           | 6                       | 0.01           |
| 8     | 1000         | 500           | 8                       | 0.02           |
| 9     | 50000        | 25000         | 1                       | 0.07           |
| 10    | 50000        | 25000         | 4                       | 0.08           |
| 11    | 50000        | 25000         | 6                       | 0.07           |
| 12    | 50000        | 25000         | 8                       | 0.08           |
| 13    | 10000        | 20000         | 1                       | 0.02           |
| 14    | 10000        | 20000         | 4                       | 0.03           |
| 15    | 10000        | 20000         | 6                       | 0.02           |
| 16    | 10000        | 20000         | 8                       | 0.02           |


---

### 🧪 Test Run 2 - Rotaru Oana-Dumitrița
- **Processor**: 2,6 GHz 6-Core Intel Core i7, 12 cores

  | Run # | Publications | Subscriptions | Parallelism (Processes) | Time (seconds) |
  |-------|--------------|---------------|-------------------------|----------------|
  | 1     | 10000        | 5000          | 1                         | 0.05           |
  | 2     | 10000        | 5000          | 4                         | 0.04           |
  | 3     | 10000        | 5000          | 6                         | 0.02           |
  | 4     | 10000        | 5000          | 8                         | 0.02           |
  | 5     | 1000         | 500           | 1                         | 0.00           |
  | 6     | 1000         | 500           | 4                         | 0.00           |
  | 7     | 1000         | 500           | 6                         | 0.01           |
  | 8     | 1000         | 500           | 8                         | 0.01           |
  | 9     | 50000        | 25000         | 1                         | 0.11           |
  | 10    | 50000        | 25000         | 4                         | 0.10           |
  | 11    | 50000        | 25000         | 6                         | 0.12           |
  | 12    | 50000        | 25000         | 8                         | 0.11           |
  | 13    | 10000        | 20000         | 1                         | 0.06           |
  | 14    | 10000        | 20000         | 4                         | 0.05           |
  | 15    | 10000        | 20000         | 6                         | 0.04           |
  | 16    | 10000        | 20000         | 8                         | 0.04           |


---

### 🧪 Test Run 3 - Sîrghi Constantin-Florin
- **Processor**: Intel(R) Core(TM) i7-10870H CPU @ 2.20GHz

| Run # | Publications | Subscriptions | Parallelism (Processes) | Time (seconds) |
|-------|--------------|---------------|-------------------------|----------------|
| 1     | 10000        | 5000          | 1                         | 0.05           |
| 2     | 10000        | 5000          | 4                         | 0.04           |
| 3     | 10000        | 5000          | 6                         | 0.02           |
| 4     | 10000        | 5000          | 8                         | 0.02           |
| 5     | 1000         | 500           | 1                         | 0.00           |
| 6     | 1000         | 500           | 4                         | 0.01           |
| 7     | 1000         | 500           | 6                         | 0.00           |
| 8     | 1000         | 500           | 8                         | 0.01           |
| 9     | 50000        | 25000         | 1                         | 0.09           |
| 10    | 50000        | 25000         | 4                         | 0.09           |
| 11    | 50000        | 25000         | 6                         | 0.05           |
| 12    | 50000        | 25000         | 8                         | 0.06           |
| 13    | 10000        | 20000         | 1                         | 0.03           |
| 14    | 10000        | 20000         | 4                         | 0.02           |
| 15    | 10000        | 20000         | 6                         | 0.02           |
| 16    | 10000        | 20000         | 8                         | 0.02           |

---

## Notes

- The field frequency percentages and `=` operator constraints are respected using deterministic distribution logic.
- Output files are stored in `publications.txt` and `subscriptions.txt`.
- Configurable parameters can be adjusted in the `Generator.java`.

---
