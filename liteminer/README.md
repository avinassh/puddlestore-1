# LiteMiner

# Method of utilizing new miners
We chose the very simple but effective method of creating more jobs than miners on the initial client request.
This ensured that if up to 2x the initial number of miners joined (that was the ratio we chose), they would have
work to do. This was a tradeoff between network utilization and the ability to spread work onto new miners, but we
figured the effect was sufficiently minimal, especially compared to the heartbeat frequency.

# Overview of Tests
- pool state/load-balancing tests
    1) all miners drop out => wait for more miners to connect!
    2) check adding miners after miner has dropped -> speed should increase
    TODO: 3) check adding miners when no miner has dropped -> speed may not increase
    4) pool is ready to accept new tasks
    5) barrage of connecting clients
    6) barrage of client mine requests
    7) many pools per client
- connection tests
    1) test basic connections
    2) check tolerance if miner drops by disconnecting
    3) check tolerance if miner drops by timing out
    4) client disconnects from pool
    5) pool disconnects from client
    6) pool disconnects from miner
- miner tests
    - note: there are some race conditions inherent in these tests, but they are dealt with with reasonable probability
    1) normal operations
    2) bounds checks
    3) interval edge cases
    4) empty string Data
    5) premature shutdown
    6) premature connection close
    7) premature connection close on pool side
    8) unexpected message from pool
- external (non-go tests)
    1) interoperability tests
    2) CPU-intensive operations (10+ miners)
    3) multiple clients
    4) various CLI tests

# Extra Features
- Many supplemental functions to aid with testing (pool state monitoring, on the fly miner management, etc.)
- A shell script to make system testing easier

# Known Bugs
- If a client sends an additional transaction request while mining, the pool will wait to fulfill it instead of sending BusyPool, because we chose to hang on the CompletedJobs list in the pool and it's not possible to select on the message channel.
- Our tests will sometimes fail when all are run at once (we gave up trying to figure out why, but we suspect it is timing issues; it would've been better to use channels to control waiting), but we've yet to see them fail when
run individually.
- Here are some we've seen fail
- TestPoolDisconnectClientMining
- TestPoolWaitOnMiners
- TestClientBarrage
- TestMineBarrage

# Disconnect Protocols
- If a client disconnects while the pool is processing its transaction,
all of the miners will still be mining the intervals. Any clients that
attempt to connect during this time should be turned away.
