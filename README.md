# Balanced Subscription & Publication Generator

This project generates balanced sets of subscriptions and publications with support for field frequency configuration, operator distribution, and parallelized generation using threads or processes.

## Team Members

- Feraru Ionuț MSD1
- Rotaru Oana-Dumitrița MISS1 G2
- Sîrghi Constantin-Florin MSD1

---

## Parallelization Details

- **Type of parallelization used**: Apache Storm native parallelism (via workers and task parallelism)
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
| 1     | 10000        | 5000          | 1                       | 0.05           |
| 2     | 10000        | 5000          | 4                       | 0.04           |
| 3     | 10000        | 5000          | 6                       | 0.02           |
| 4     | 10000        | 5000          | 8                       | 0.03           |
| 5     | 1000         | 500           | 1                       | 0.00           |
| 6     | 1000         | 500           | 4                       | 0.00           |
| 7     | 1000         | 500           | 6                       | 0.00           |
| 8     | 1000         | 500           | 8                       | 0.00           |
| 9     | 50000        | 25000         | 1                       | 0.10           |
| 10    | 50000        | 25000         | 4                       | 0.11           |
| 11    | 50000        | 25000         | 6                       | 0.09           |
| 12    | 50000        | 25000         | 8                       | 0.10           |
| 13    | 10000        | 20000         | 1                       | 0.06           |
| 14    | 10000        | 20000         | 4                       | 0.03           |
| 15    | 10000        | 20000         | 6                       | 0.03           |
| 16    | 10000        | 20000         | 8                       | 0.03           |



---

### 🧪 Test Run 3 - Sîrghi Constantin-Florin
- **Processor**: Intel(R) Core(TM) i7-10870H CPU @ 2.20GHz

| Run # | Publications | Subscriptions | Parallelism (Processes) | Time (seconds) |
|-------|--------------|---------------|--------------------------|----------------|
| 1     | 10,000       | 5,000         | 1                        | 3.45           |
| 2     | 10,000       | 5,000         | 4                        | 3.28           |
| 3     | 10,000       | 5,000         | 6                        | 3.33           |
| 4     | 10,000       | 5,000         | 8                        | 3.33           |
| 5     | 50,000       | 25,000        | 1                        | 3.37           |
| 6     | 50,000       | 25,000        | 4                        | 3.28           |
| 7     | 50,000       | 25,000        | 6                        | 3.31           |
| 8     | 50,000       | 25,000        | 8                        | 3.37           |
| 9     | 10,000       | 20,000        | 1                        | 3.29           |
| 10    | 10,000       | 20,000        | 4                        | 3.42           |
| 11    | 10,000       | 20,000        | 6                        | 3.40           |
| 12    | 10,000       | 20,000        | 8                        | 3.39           |


---

## Notes

- The field frequency percentages and `=` operator constraints are respected using deterministic distribution logic.
- Output files are stored in `publications.txt` and `subscriptions.txt`.
- Configurable parameters can be adjusted in the `GeneratorTopology.java`.

---
