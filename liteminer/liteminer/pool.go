/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: a LiteMiner mining pool.
 */

package liteminer

import (
	"encoding/gob"
	"io"
	"math"
	"net"
	"sync"
	"time"
)

const HEARTBEAT_TIMEOUT = 3 * HEARTBEAT_FREQ
const INTERVAL_MINER_RATIO = 2
const MAX_MINING_MINERS = 100

// Job represents a job for a miner to work on
// (this abstraction was kept in order to easily accomodate new
//  features. We were planning on these but it was discouraged
//  Piazza (keeping track of miner progress in jobs, etc.))
type Job struct {
	Intrvl Interval // Interval represented by the job
}

// Pool represents a LiteMiner mining pool
type Pool struct {
	Addr          net.Addr                // Address of the pool
	Miners        map[net.Addr]MiningConn // Currently connected miners
	Client        MiningConn              // The current client
	Data          string                  // the string provided by the client; hash target
	busy          bool                    // True when processing a transaction
	mutex         sync.Mutex              // To manage concurrent access to these members
	OpenJobs      chan Job                // Channel for open Jobs
	CompletedJobs chan Job                // Channel for Miners to submit open work on
}

// CreatePool creates a new pool at the specified port.
func CreatePool(port string) (pp *Pool, err error) {
	var pool Pool

	pp = &pool

	pool.Client.Conn = nil
	pool.Data = ""
	pool.busy = false
	pool.Miners = make(map[net.Addr]MiningConn)
	pool.OpenJobs = make(chan Job, MAX_MINING_MINERS)
	pool.CompletedJobs = nil

	err = pp.startListener(port)

	return
}

// startListener starts listening for new connections.
func (p *Pool) startListener(port string) (err error) {
	listener, portId, err := OpenListener(port)
	if err != nil {
		return
	}

	p.Addr = listener.Addr()

	Out.Printf("Listening on port %v\n", portId)

	// Listen for and accept connections
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				Err.Printf("Received error %v when listening for connections\n", err)
				continue
			}

			go p.handleConnection(conn)
		}
	}()

	return
}

// handleConnection handles an incoming connection and delegates to
// handleMinerConnection or handleClientConnection.
func (p *Pool) handleConnection(nc net.Conn) {
	// Set up connection
	conn := MiningConn{}
	conn.Conn = nc
	conn.Enc = gob.NewEncoder(nc)
	conn.Dec = gob.NewDecoder(nc)

	// Wait for Hello message
	msg, err := RecvMsg(conn)
	if err != nil {
		Err.Printf(
			"Received error %v when processing Hello message from %v\n",
			err,
			conn.Conn.RemoteAddr(),
		)
		conn.Conn.Close() // Close the connection
		return
	}

	switch msg.Type {
	case MinerHello:
		p.handleMinerConnection(conn)
	case ClientHello:
		p.handleClientConnection(conn)
	default:
		Debug.Printf("Pool %v received unexpected msg %v from %v\n", p.Addr, msg.Type, conn.Conn.RemoteAddr())
		SendMsg(conn, ErrorMsg("Unexpected message type"))
	}
}

// handleClientConnection handles a connection from a client.
func (p *Pool) handleClientConnection(conn MiningConn) {
	Debug.Printf("Received client connection from %v", conn.Conn.RemoteAddr())

	p.mutex.Lock()
	if p.Client.Conn != nil {
		Debug.Printf(
			"Busy with client %v, sending BusyPool message to client %v",
			p.Client.Conn.RemoteAddr(),
			conn.Conn.RemoteAddr(),
		)
		SendMsg(conn, BusyPoolMsg())
		p.mutex.Unlock()
		return
	} else if p.busy {
		Debug.Printf(
			"Busy with previous transaction, sending BusyPool message to client %v",
			conn.Conn.RemoteAddr(),
		)
		SendMsg(conn, BusyPoolMsg())
		p.mutex.Unlock()
		return
	}
	p.Client = conn
	p.mutex.Unlock()
	// we are not busy as we currently have not received a transaction request

	// Listen for and handle incoming messages
	for {
		msg, err := RecvMsg(conn)
		if err != nil {
			// if error is found while receiving messages
			if _, ok := err.(*net.OpError); ok || err == io.EOF {
				p.DisconnectClient(conn)
				return
			}
			Err.Printf(
				"Received error %v when processing message from client %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)
			p.reset()
			continue
		}

		// filters out messages that are not transactions
		if msg.Type != Transaction {
			SendMsg(conn, ErrorMsg("Expected Transaction message"))
			p.reset()
			continue
		}

		Debug.Printf(
			"Received transaction from client %v with data %v and upper bound %v",
			conn.Conn.RemoteAddr(),
			msg.Data,
			msg.Upper,
		)

		// handles case where no miners are connected
		p.mutex.Lock()
		if len(p.Miners) == 0 {
			SendMsg(conn, ErrorMsg("No miners connected"))
			p.mutex.Unlock()
			p.reset()
			continue
		}
		p.mutex.Unlock()

		// filters out multiple transaction requests from same client
		p.mutex.Lock()
		if p.busy {

		} else {
			// transaction is accepted!
			Debug.Printf("Processing transaction from %v", conn.Conn.RemoteAddr())
			// we are now busy
			p.busy = true
			numIntervals := INTERVAL_MINER_RATIO * len(p.Miners)
			intervals := GenerateIntervals(msg.Upper, numIntervals)
			p.CompletedJobs = make(chan Job, numIntervals)
			p.Data = msg.Data
			p.mutex.Unlock()
			Debug.Printf("Generated %d intervals to find best hash of '%s'", numIntervals, p.Data)

			// pushes all Jobs onto the OpenJobs channel
			for _, ivl := range intervals {
				j := Job{ivl}
				p.OpenJobs <- j
			}

			Out.Printf("Pushed all %d Jobs onto OpenJobs for %v", numIntervals, conn.Conn.RemoteAddr())

			// TODO: attempted bugfix
			//// handle a request while we're waiting for miner completion
			//go func() {
			//	for p.busy {
			//		tmpMsg, tmpErr := RecvMsg(conn)
			//		if tmpErr == nil && tmpMsg.Type == Transaction {
			//			// handles BusyPool above at the top of the method
			//			Debug.Printf("multiple transaction requests received from %v", conn.Conn.RemoteAddr())
			//			// if busy, let them know and ignore mining request
			//			SendMsg(conn, BusyPoolMsg())
			//		}
			//	}
			//}()

			// wait on mining miners and calculate best result as they finish
			// (we know that we'll get numIntervals that will finish)
			bestHash := uint64(math.MaxUint64)
			bestNonce := uint64(0)
			for i := 0; i < numIntervals; i++ {
				// waits until a miner finishes
				j := <-p.CompletedJobs
				ivl := j.Intrvl
				Out.Printf("Received completed job for %v. %v/%v jobs completed",
					conn.Conn.RemoteAddr(), i+1, numIntervals)
				if ivl.Hash <= bestHash {
					bestHash = ivl.Hash
					bestNonce = ivl.Nonce
				}
			}

			Out.Printf("Finished transaction for %v. Nonce, Hash returned: %v, %v", conn.Conn.RemoteAddr(), bestNonce, bestHash)
			SendMsg(conn, ProofOfWorkMsg(msg.Data, bestNonce, bestHash))

			p.reset()
		}
	}
}

// Disconnecting pool entirely
func (p *Pool) DisconnectClient(conn MiningConn) {
	Out.Printf(
		"Disconnected from client %v; Pool resetting\n",
		conn.Conn.RemoteAddr(),
	)
	p.mutex.Lock()
	conn.Conn.Close() // Close the connection
	p.Client.Conn = nil
	p.Data = ""
	p.busy = false
	p.mutex.Unlock()
}

// Reset Pool to waiting state
func (p *Pool) reset() {
	p.mutex.Lock()
	Out.Printf("Setting pool to waiting state\n")
	p.Data = ""
	p.busy = false
	p.mutex.Unlock()
}

// helper function to remove miner from those available
func (p *Pool) removeMiner(conn MiningConn, curJob *Job) {
	p.mutex.Lock()
	delete(p.Miners, conn.Conn.RemoteAddr())
	p.mutex.Unlock()
	conn.Conn.Close()
	// return our job
	p.OpenJobs <- *curJob
	Debug.Printf("Disconnected miner %v added job back", conn.Conn.RemoteAddr())
}

// handleMinerConnection handles a connection from a miner.
func (p *Pool) handleMinerConnection(conn MiningConn) {

	// run once every time when a miner connects
	Debug.Printf("Received miner connection from %v", conn.Conn.RemoteAddr())

	p.mutex.Lock()
	p.Miners[conn.Conn.RemoteAddr()] = conn
	p.mutex.Unlock()

	msgChan := make(chan *Message)
	// runs forever in background and fills msgChan
	go p.receiveFromMiner(conn, msgChan)

	// Students should handle a miner connection. If a miner does not
	// send a StatusUpdate message every HEARTBEAT_TIMEOUT while mining,
	// any work assigned to them should be redistributed and they should be
	// disconnected and removed from p.Miners.

	// variable to determine the miner state - ready or mining
	mining := false
	// pointer storing
	curJob := new(Job)

	for {
		/* WAITING STAGE */

		Debug.Printf("Miner %v searching for new jobs...", conn.Conn.RemoteAddr())

		// wait until we get a new job
		newJob := <-p.OpenJobs

		Debug.Printf("Miner %v accepting job", conn.Conn.RemoteAddr())
		// take this job!
		curJob = &newJob
		mining = true
		// send a message to start mining!
		SendMsg(conn, MineRequestMsg(p.Data, newJob.Intrvl.Lower, newJob.Intrvl.Upper))

		// create timer so it can start monitoring for lack of heartbeats
		killTimer := time.NewTimer(HEARTBEAT_TIMEOUT)

		/* MINING STAGE */

		for mining {
			select {
			case <-killTimer.C:
				// miner is dead, remove ourselves and close connection
				Debug.Printf("Miner %v terminated", conn.Conn.RemoteAddr())
				p.removeMiner(conn, curJob)
				// exit function
				return

			case msg := <-msgChan:
				killTimer.Reset(HEARTBEAT_TIMEOUT) // reset timer right after heartbeat

				if msg == nil {
					Out.Printf("Miner %v disconnected", conn.Conn.RemoteAddr())
					p.removeMiner(conn, curJob)
					return
				}

				if msg.Type == ProofOfWork {
					// process results of finished interval
					Debug.Printf("Miner %v completed interval", conn.Conn.RemoteAddr())
					mining = false
					curJob.Intrvl.Nonce = msg.Nonce
					curJob.Intrvl.Hash = msg.Hash
					// note: even though this pointer is changed on every transaction request,
					// because we only handle one at a time (after all miners have completed),
					// we know that when a miner hits this line, the CompletedJobs list
					// has been initalized as expected, because they were all hanging
					// on <-p.OpenJobs when it was
					p.CompletedJobs <- Job{curJob.Intrvl} // send to central task
				}
			}
		}
	}
}

// receiveFromMiner waits for messages from the miner specified by conn and
// forwards them over msgChan.
func (p *Pool) receiveFromMiner(conn MiningConn, msgChan chan *Message) {
	for {
		msg, err := RecvMsg(conn)
		if err != nil {
			if _, ok := err.(*net.OpError); ok || err == io.EOF {
				// send a nil over the channel to indicate the goroutine returns
				// (the miner connection handler will remove the miner)
				msgChan <- nil
				return
			}
			Err.Printf(
				"Received error %v when processing message from miner %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)
			continue
		}
		msgChan <- &msg
	}
}

// GetMiners returns the addresses of any connected miners.
func (p *Pool) GetMiners() []net.Addr {
	miners := []net.Addr{}
	p.mutex.Lock()
	for _, m := range p.Miners {
		miners = append(miners, m.Conn.RemoteAddr())
	}
	p.mutex.Unlock()
	return miners
}

// GetClient returns the address of the current client or nil if there is no
// current client.
func (p *Pool) GetClient() net.Addr {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.Client.Conn == nil {
		return nil
	}
	return p.Client.Conn.RemoteAddr()
}
