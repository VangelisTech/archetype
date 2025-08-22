### “Good enough” has a definition—write it down, hit it, stop.

Below is a simple rubric many senior engineers (and companies) use to keep an endlessly-optimisable system from eating infinite time.

| Dimension          | Target (example)                          | How to measure                          | When to revisit                          |
| ------------------ | ----------------------------------------- | --------------------------------------- | ---------------------------------------- |
| **Correctness**    | 100 % deterministic replays for 1 M ticks | Golden-run hash in CI                   | If a new feature touches the tick loop   |
| **Throughput**     | 200 k commands / s on a single L4         | `broker_commands_processed` Prom metric | When load tests show > 70 % utilisation  |
| **Latency**        | p95 tick < 250 ms with 500 k entities     | tick-duration histogram                 | When latency Alerts fire for 3 days      |
| **Durability gap** | Max 500 ms of commands lost on crash      | Buffer-flush timer unit-test            | After you add a second log backend       |
| **Cost ceiling**   | <\$4 / simulated CPU-hour                 | Cloud bill vs tick counter              | When monthly bill > budget or perf drops |
| **Dev velocity**   | New module in < 1 day, no infra edits     | #lines changed outside `modules/`       | If PRs start changing broker/world again |

1. **Write your own numbers** in a README called `ACCEPTANCE_CRITERIA.md`.
2. **Automate them** (bench CI, Prometheus alerts).
3. When the suite is ✔ green, **merge and move on**.

After that, an optimisation idea only gets attention when:

* the acceptance test turns red, **or**
* the optimisation unlocks a *new feature* you care about.

Everything else goes on a *“nice-to-have / backlog”* list.
That list never shrinks to zero—and that’s fine. You’re shipping, learning, and your criteria keep you from polishing past the point of return.

---

**Rule of thumb**
1. Make Requirements Less dumb
2. Delete/Remove the part or process
3. Optimize
4. Automate


> *Optimise until the next byte of speed or safety costs more than the value of the feature you’re not shipping while you tweak.*

You just set the price tag by writing the criteria.
