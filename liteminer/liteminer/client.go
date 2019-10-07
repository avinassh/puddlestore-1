/*
 *  Brown University, CS138, Spring 2018
 *
 *  Purpose: a LiteMiner client.
 */

package liteminer

import (
	"fmt"
	"io"
	"net"
	"sync"
	"errors"
)

// Represents a LiteMiner client
type Client struct {
	PoolConns map[net.Addr]MiningConn // Pool(s) that the client is currently connected to
	Nonces    map[net.Addr]int64      // Nonce(s) received by the various pool(s) for the current transaction
	TxResults chan map[net.Addr]int64 // Used to send results of transaction
	mutex     sync.Mutex              // To manage concurrent access to these members
}

// CreateClient creates a new client connected to the given pool addresses.
func CreateClient(addrs []string) (cp *Client) {
	var client Client

	cp = &client

	client.PoolConns = make(map[net.Addr]MiningConn)
	client.Nonces = make(map[net.Addr]int64)
	client.TxResults = make(chan map[net.Addr]int64)

	cp.Connect(addrs)

	return
}

// GetConn loops over the PoolConns for a given Addr and
// returns the MiningConn if it finds one. Returns an error
// if the addr is not present in PoolConns. Threadsafe.
func (c *Client) GetConn(addr net.Addr) (conn MiningConn, err error) {

	c.mutex.Lock()
	err = nil
	for c_addr, c_conn := range c.PoolConns  {
		if addr.String() == c_addr.String() {
			conn = c_conn
			c.mutex.Unlock()
			return
		}
	}
	err = errors.New("could not find address")
	c.mutex.Unlock()
	return
}

// GetNonce loops over the Nonces for a given Addr and
// returns the nonce corresponding if it finds one. Returns an error
// if the addr is not present in PoolConns. Threadsafe.
func (c *Client) GetNonce(addr net.Addr) (nonce int64, err error) {

	c.mutex.Lock()
	err = nil
	for c_addr, c_nonce := range c.Nonces  {
		if addr.String() == c_addr.String() {
			nonce = c_nonce
			c.mutex.Unlock()
			return
		}
	}
	err = errors.New("could not find address")
	c.mutex.Unlock()
	return
}

// Disconnect from the address specified, if present. If not, do nothing.
func (c *Client) Disconnect(addr net.Addr) {
	conn, err := c.GetConn(addr)
	c.mutex.Lock()
	if err == nil {
		Out.Printf("client disconnected from pol %v", addr)
		conn.Conn.Close()
	}
	c.mutex.Unlock()
	return
}

// Connect connects the client to the specified pool addresses.
func (c *Client) Connect(addrs []string) {
	// iterates over the addrs array, ignoring indices in _ and assigning values
	// to addr
	for _, addr := range addrs {
		conn, err := ClientConnect(addr)
		if err != nil {
			Err.Printf("Received error %v when connecting to pool %v\n", err, addr)
			continue
		}

		c.mutex.Lock()
		c.PoolConns[conn.Conn.RemoteAddr()] = conn
		c.mutex.Unlock()

		go c.processPool(conn)
	}
}

// processPool handles incoming messages from the pool represented by conn.
func (c *Client) processPool(conn MiningConn) {
	// loops forever
	for {
		msg, err := RecvMsg(conn)
		if err != nil {
			if err == io.EOF {
				Err.Printf("Lost connection to pool %v\n", conn.Conn.RemoteAddr())

				c.mutex.Lock()
				delete(c.PoolConns, conn.Conn.RemoteAddr())
				if len(c.Nonces) == len(c.PoolConns) {
					c.TxResults <- c.Nonces
				}
				c.mutex.Unlock()

				conn.Conn.Close() // Close the connection

				return
			}

			Err.Printf(
				"Received error %v when processing pool %v\n",
				err,
				conn.Conn.RemoteAddr(),
			)

			c.mutex.Lock()
			c.Nonces[conn.Conn.RemoteAddr()] = -1 // -1 used to indicate error
			if len(c.Nonces) == len(c.PoolConns) {
				c.TxResults <- c.Nonces
			}
			c.mutex.Unlock()

			continue
		}

		switch msg.Type {
		case BusyPool:
			Out.Printf("Pool %v is currently busy, disconnecting\n", conn.Conn.RemoteAddr())

			c.mutex.Lock()
			delete(c.PoolConns, conn.Conn.RemoteAddr())
			c.mutex.Unlock()

			conn.Conn.Close() // Close the connection

			return
		case ProofOfWork:
			Debug.Printf("Pool %v found nonce %v\n", conn.Conn.RemoteAddr(), msg.Nonce)

			c.mutex.Lock()
			c.Nonces[conn.Conn.RemoteAddr()] = int64(msg.Nonce)
			if len(c.Nonces) == len(c.PoolConns) {
				c.TxResults <- c.Nonces
			}
			c.mutex.Unlock()
		default:
			Err.Printf(
				"Received unexpected message %v of type %v from pool %v\n",
				msg.Data,
				msg.Type,
				conn.Conn.RemoteAddr(),
			)

			c.mutex.Lock()
			c.Nonces[conn.Conn.RemoteAddr()] = -1 // -1 used to indicate error
			if len(c.Nonces) == len(c.PoolConns) {
				c.TxResults <- c.Nonces
			}
			c.mutex.Unlock()
		}
	}
}

// Given a transaction encoded as a string and an unsigned integer, Mine returns
// the nonce(s) calculated by any connected pool(s). This method should NOT be
// executed concurrently by the same miner.
func (c *Client) Mine(data string, upperBound uint64) (map[net.Addr]int64, error) {
	c.mutex.Lock()

	if len(c.PoolConns) == 0 {
		c.mutex.Unlock()
		return nil, fmt.Errorf("Not connected to any pools")
	}

	c.Nonces = make(map[net.Addr]int64)

	// Send transaction to connected pool(s)
	tx := TransactionMsg(data, upperBound)
	for _, conn := range c.PoolConns {
		SendMsg(conn, tx)
	}
	c.mutex.Unlock()

	nonces := <-c.TxResults

	return nonces, nil
}
