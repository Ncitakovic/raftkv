# RaftKV – Working Rules

- Java 21, Maven via the wrapper: always use `./mvnw` (or `mvnw.cmd` on Windows), never a global `mvn`. `./mvnw verify` must pass before every commit.
- Each step is done on a separate branch `milestone-2/<step-name>` and goes through a pull request.
- Commit messages: short title + description of what and why. No Co-Authored-By or Claude-Session lines.
- All new logic gets unit tests. Tests must not use Thread.sleep – time is injected through a Clock/Scheduler interface.
- All Raft state is modified by a single thread (single-threaded executor).
- Durability rule: currentTerm and votedFor are written to disk BEFORE responding to any RPC (see PersistentState).
- After each step, update docs/DESIGN.md (section 3) and the roadmap table in README.md.
- Do not modify files in target/.
- The milestone 2 plan is in docs/DESIGN.md, section 3.
