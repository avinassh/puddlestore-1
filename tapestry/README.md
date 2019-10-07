# Tapestry

## Overview of Tests
**NOTE: most of our unit tests require these settings (and will fail otherwise):**
```
BASE = 16
DIGITS = 4
```
These settings make things faster/cleaner:
```
REPUBLISH = 500 * time.Millisecond
TIMEOUT = 1500 * time.Millisecond
DEBUG_ON_TESTING = true // in oracle.go
```
Also, neither of us had Macs, but our oracle will very likely need a large `ulimit -n`

## Testing details:
- ID handling testing
    - ID helper functions testing
    - Routing table updating testing
    - Add, remove (test how modify table)
- Routing testing
    - getNextHop()
    - findRoot()
        - edge cases (short root paths)
        - string of RemoteNode failures (rerouting)
        - complete failure of path to root
        - findRoot hop numbers
    - Lookup tests (oracle)
        - Tested a lot via findRoot() tests
- Adding nodes/data tests
    - Make sure publishing data updates its root node
- Removing nodes/data tests
    - graceful vs. non graceful exits
    - Make sure timeouts work
- Fault-tolerance tests
    - RPC call failures
    - Make sure routing works after dropping nodes
    - Make sure data is copied (oracle Remove/Fetch)
- Global Oracle Test
    - Random generation of sequences of Adds, Kills, Leaves, Publishes, Removes, and Lookups with checks for all of them
    - Specific sequence tests for well-defined edge cases (using framework)
    - Register, Fetch work as expected
    - NOTE: the oracle could be stricter, but the randomness leads to uncertainty such that we can't be too strict in our tests (without knowing more information)

## Extra Features
- An oracle framework for creating arbitrary sequences of testing command and automatically validating them (this made it easy to write system tests)
- Nodes will try multiple times to add themselves to the network (this helped our network weather massive node die-offs in tests)

## Known Bugs
- Occasionally, because there's always a small chance, a node will randomly be added with the same ID as an existing node, which is an error. We chose to keep this as an error, but it's very unlikely.
- Occasionally, there will be random connection failures at very critical points where these are errors (sufficient failures to defeat retries, for example)
- The TestRandomSequence and occasionally TestMultipleTransactionMultipleNodes (and others) will sometimes fail to lookup nodes after failures. We suspect this is due to the republishing today and in the conditions where the root node is lost (killed) and the network cannot propogate fast enough.
