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
| 1     | 10000        | 5000          | 1                       | 4.20           |
| 2     | 10000        | 5000          | 4                       | 3.32           |
| 3     | 10000        | 5000          | 6                       | 3.27           |
| 4     | 10000        | 5000          | 8                       | 3.21           |
| 5     | 50000        | 25000         | 1                       | 3.11           |
| 6     | 50000        | 25000         | 4                       | 3.21           |
| 7     | 50000        | 25000         | 6                       | 3.21           |
| 8     | 50000        | 25000         | 8                       | 3.24           |
| 9     | 10000        | 20000         | 1                       | 3.21           |
| 10    | 10000        | 20000         | 4                       | 3.23           |
| 11    | 10000        | 20000         | 6                       | 3.13           |
| 12    | 10000        | 20000         | 8                       | 3.12           |


---

### 🧪 Test Run 2 - Rotaru Oana-Dumitrița
- **Processor**: 2,6 GHz 6-Core Intel Core i7

| Run # | Publications | Subscriptions | Parallelism (Processes) | Time (seconds) |
|-------|--------------|---------------|--------------------------|----------------|
| 1     | 10,000       | 5,000         | 1                        | 8.98           |
| 2     | 10,000       | 5,000         | 4                        | 8.89           |
| 3     | 10,000       | 5,000         | 6                        | 8.97           |
| 4     | 10,000       | 5,000         | 8                        | 8.94           |
| 5     | 50,000       | 25,000        | 1                        | 9.92           |
| 6     | 50,000       | 25,000        | 4                        | 9.91           |
| 7     | 50,000       | 25,000        | 6                        | 8.88           |
| 8     | 50,000       | 25,000        | 8                        | 9.92           |
| 9     | 10,000       | 20,000        | 1                        | 8.94           |
| 10    | 10,000       | 20,000        | 4                        | 8.97           |
| 11    | 10,000       | 20,000        | 6                        | 8.98           |
| 12    | 10,000       | 20,000        | 8                        | 8.88           |


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