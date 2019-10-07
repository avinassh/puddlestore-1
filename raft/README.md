# Raft

## Important Notes
- There is a strictTest field in a RaftCluster which, when enabled,
 runs tests that are only expected to pass in normal operation, not under stress.
 It is enabled in some "normal" tests and may lead to occasional failures.
    - **These tests in the TestRandomSequenceNormal in particular may fail occasionally!!**
- Because our tests manually remove the raftlogs/ directory, they may not be runnable in parallel.
  I.e. they may need the `-parallel 0` flag.

## Overview of Tests
-- UNIT --
 - mergeLogEntries
 - shouldVoteFor

-- ORACLE --
 - Allows random or specific application of high-level commands
   (add client, send request, partition network, etc.), including assertions
   about each one executed constantly (see _assertLeaderState_ and _assertLogStates_)
 - Several specific situations/edge cases implemented with this framework
 - Running this over many many cases ensures we essentially hit everything! :)
 - Oracle asserts required properties about the leader (or alternatively, set of candidates)
    and the logs of the cluster after each event, noting errors if anything is inconsistent.
 - General test cases covered:
    - Normal case; only client additions/requests/departures
    - Node exit case; nodes randomly shutdown and come back (using raftlogs)
    - Partition case; different types of partitions (random subset of connections or specific halving partition) are randomly created and removed

-- SYSTEM TESTS --

TEST VARIOUS CONCRETE EDGE CASES -- we ran some of these tests with the
oracle framework and some from your CLI. The CLI tests have been documented in detail here
and the oracle framework tests are written in system_test.go

1. TestLeaderDropWithLargeLog
The following commands were run in the command line interface
Nodes A,B,C: A is leader; client is connected to A
- B: disable all
- C: disable all
- client: init (this adds some entries to A's log)
- A: disable all
- B: enable all
- C: enable all [now B is the leader and B/C are in sync]
- A: enable all
We may observe the log is truncated appropriately, and the test passes!!

2. TestSplitVotes - tests that when all nodes timeout, one of them does not get elected immediately.

3. TestOutdatedLogCannotElect - tests that a node with an outdated log will receive no votes.
This test also tests the rejoining of a single node (follower) partition from the rest of the cluster

The following commands were run in the command line interface
Nodes A,B,C: A is leader; client connected to A
- B: disable all [B will go into continous term incrementation loop]
- many sequences of hashes from the client, so that A and B have matchings logs of the form:
Node 61670 LogCache:
 idx:0, term:0
 idx:1, term:1
 idx:2, term:1
 idx:3, term:1
 idx:4, term:1
 idx:5, term:1
 idx:6, term:4
- In the meanwhile, B has log of the form:
Node 12074 LogCache:
 idx:0, term:0
 idx:1, term:1
- B: enable all [B will run for reelection] and should receive no votes!
(12074) Transitioning to CANDIDATE_STATE
(12074) Setting current term from 32 -> 33
(12074/candidate) Finished election with 1/3 votes
- We observe that a large sequence of merging logs takes place and eventually B's log matches that of A

4. TwoLeaderPartitionTest - tests that in situations where there is a partition with two independent leaders, 
the leader of the smaller partition steps down when the partition is healed.
Nodes A,B,C: A is leader; client connected to A
- A: disable all
- client attempts to connect to A [but fails since it is a minority]
- B is elected leader of other side of partition and client connects to it
- client performs sequence of commands to increase B's log
- we see that B and C's log are mirrored and committed

---A's log----
Node 57939 LogCache:
 idx:0, term:0
 idx:1, term:1
 idx:2, term:1
 idx:3, term:1
Current commit index: 1

---B+C's log---
Node 681 LogCache:
 idx:0, term:0
 idx:1, term:1
 idx:2, term:2
 idx:3, term:2
 idx:4, term:2
Current commit index: 4

- A: enable all
- A's extra log entries are scrapped and after enabling we have:

---A's log----
Node 57939 LogCache:
 idx:0, term:0
 idx:1, term:1
 idx:2, term:2
 idx:3, term:2
 idx:4, term:2


## Extra Features
- Powerful oracle testing framework to facilitate testing

## Known Bugs
- TA tests TestRequestVotesRPC, TestStatePersistenceAfterShutdown only work alone
- TestLeaderDropWithLargeLog is having some infinite loop issues;
   we suspect it may not actually be a reasonable test, or possibly there may
   be a subtle shutdown race condition.
- TestLeaderUpdatesFollowerNextTermExtraEntries doesn't work sometimes IF run alongside other tests
- We might have some issues with parallel tests removing eachother's logs
  from under them, but this should be solved by not parallelizing tests