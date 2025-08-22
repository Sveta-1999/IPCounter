# IPCounter
Go tool to count unique IPv4 addresses from large files, with naive, concurrent, and bucket-based methods.

Supports:
- **naive** – simple map-based method (small/medium files)
- **concurrent** – multi-core bitset method (large files)
- **bucket** – two-pass low-memory method (huge files)

## Usage
```bash
go run . -impl naive <filename>
go run . -impl concurrent <filename>
go run . -impl bucket <filename>
