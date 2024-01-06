package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const MAX_VALUE = math.MaxInt32

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	maxseq         int            // Max sequence seen so far
	proposalNumber int            // Keeping track of the proposal number for this peer
	seqStatus      map[int]Status // Seq -> Status
	doneTracker    map[int]int    // peer[i] -> Max Done sequence
}

type Request struct {
	SequenceNumber int
	ProposalNumber int
	ProposalValue  interface{} // Reusing this for prepare, accept and decide phase so this can be nil
}

type Response struct {
	Promise            int
	MaxAcceptedRequest Request
	MaxDoneValue       int //Local done value of this peer
}

type Status struct {
	Fate                  Fate
	highestProposalNumber int     // Using this for prepare Phase
	currentRequest        Request // Using this for accept and decide Phase
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

func (px *Paxos) SendPrepare(request *Request) (bool, interface{}) {
	// Your code here.

	px.mu.Lock()
	count := len(px.peers)
	px.mu.Unlock()

	var majority = (count / 2) + 1

	var response Response

	// For keeping track of previously accepted requests by peers
	highestRequest := Request{request.SequenceNumber, -1, request.ProposalValue}

	// Keeping track of the proposal number of accepted requests in case of peer rejects prepare
	higherProposalNumber := request.ProposalNumber

	promiseCount := 0

	for i := 0; i < count; i++ {

		success := true
		// From the hints, make local call for self (Saves RPC count as well !)
		// IN order to pass the tests assuming unrelaible network, paxos is calling
		// the local acceptor through a function rather than a RPC.
		if i == px.me {
			px.HandlePrepare(request, &response)
		} else {
			success = call(px.peers[i], "Paxos.HandlePrepare", *request, &response)
		}

		if success == true {
			px.mu.Lock()

			// Update the piggybacked done values
			currDone := px.doneTracker[i]
			if response.MaxDoneValue > currDone {
				px.doneTracker[i] = response.MaxDoneValue
			}
			px.mu.Unlock()

			if response.Promise == 0 {
				promiseCount += 1
				if response.MaxAcceptedRequest.ProposalNumber > highestRequest.ProposalNumber {
					highestRequest.ProposalValue = response.MaxAcceptedRequest.ProposalValue
					highestRequest.ProposalNumber = response.MaxAcceptedRequest.ProposalNumber
				}
			} else {
				if response.MaxAcceptedRequest.ProposalNumber > higherProposalNumber {
					higherProposalNumber = response.MaxAcceptedRequest.ProposalNumber
				}
			}
		}
	}

	// If any of the host sent back a previously accepted value, it is stored in highestReq
	if promiseCount >= majority {
		// Majority is reached
		return true, highestRequest.ProposalValue
	}
	// Majority not reached
	px.mu.Lock()
	// Update the proposal number to go beyond the highest proposal number
	// as returned by peers. Improves the performance
	px.proposalNumber = (higherProposalNumber+count)/count*count + px.me
	px.mu.Unlock()
	return false, nil
}

func (px *Paxos) HandlePrepare(req *Request, res *Response) error {
	seq := req.SequenceNumber

	px.mu.Lock()
	seqStatus, exists := px.seqStatus[seq]
	selfDoneValue, _ := px.doneTracker[px.me]

	if exists {
		if req.ProposalNumber > seqStatus.highestProposalNumber {
			// Current proposal number is higher than what was previously seen
			seqStatus.highestProposalNumber = req.ProposalNumber
			px.seqStatus[seq] = seqStatus

			res.MaxAcceptedRequest = px.seqStatus[seq].currentRequest
			res.Promise = 0
		} else {
			// Current proposal number is lower than what was previously seen
			// Returning the highest Seen proposal number
			res.Promise = 1
			res.MaxAcceptedRequest = Request{seq, seqStatus.highestProposalNumber, nil}
		}
	} else {
		// Not seen any proposals, so update status accordingly
		newStatus := Status{
			Pending,
			req.ProposalNumber,
			Request{seq, -1, nil},
		}
		px.seqStatus[seq] = newStatus
		res.MaxAcceptedRequest = px.seqStatus[seq].currentRequest
		res.Promise = 0
	}
	// Paxos is piggybacking the done values in the agreement protocol packets
	// p1 can learn from p2's last done value by sending an agreement message
	// After this learning process, we can then free the memory.
	// Piggyback this peer's done value
	res.MaxDoneValue = selfDoneValue
	px.mu.Unlock()
	return nil
}

func (px *Paxos) SendAccept(req *Request) bool {
	// Your code here.

	var peersCnt int

	px.mu.Lock()
	peersCnt = len(px.peers)
	px.mu.Unlock()

	var majority int
	var response Response

	majority = (peersCnt / 2) + 1
	promiseCount := 0

	for i := 0; i < peersCnt; i++ {
		var ret bool
		ret = true
		// From hints
		if i == px.me {
			px.HandleAccept(req, &response)
		} else {
			ret = call(px.peers[i], "Paxos.HandleAccept", *req, &response)
		}
		if ret == true {
			if response.Promise == 0 {
				promiseCount += 1
			}
		} else {
			px.mu.Lock()
			if response.MaxAcceptedRequest.ProposalNumber > px.proposalNumber {
				px.proposalNumber = (response.MaxAcceptedRequest.ProposalNumber+peersCnt)/peersCnt*peersCnt + px.me
			}
			px.mu.Unlock()
		}
	}

	// Here we are not updating the request...
	// The peer needs to retry from its prepare phase again
	if promiseCount >= majority {
		return true
	}

	return false
}

func (px *Paxos) HandleAccept(req *Request, res *Response) error {

	px.mu.Lock()
	status, exists := px.seqStatus[req.SequenceNumber]

	if exists {
		if req.ProposalNumber >= status.highestProposalNumber {
			// A value was previously Decided, but we should not update its state to Pending.
			// However its important for the proposer to make progress.
			// So if the proposer is proposing same value which is decided, just accept it but don't change the fate
			if status.Fate == Decided && req.ProposalValue == status.currentRequest.ProposalValue {
				res.Promise = 0
				px.mu.Unlock()
				return nil
			}

			// Accept the accept request
			status.highestProposalNumber = req.ProposalNumber
			status.currentRequest.ProposalNumber = req.ProposalNumber
			status.currentRequest.ProposalValue = req.ProposalValue
			status.Fate = Pending
			px.seqStatus[req.SequenceNumber] = status
			res.Promise = 0
		} else {
			// Proposal number is not higher than that of current accepted request
			res.MaxAcceptedRequest = status.currentRequest
			res.MaxAcceptedRequest.ProposalNumber = status.highestProposalNumber
			res.Promise = 1
		}
	} else {
		// This peer has not seen any prepare yet !
		res.Promise = 1
	}

	px.mu.Unlock()
	return nil
}

func (px *Paxos) InformDecided(request *Request) {
	var response Response
	var peersCnt int

	px.mu.Lock()
	peersCnt = len(px.peers)
	px.mu.Unlock()

	// Just try to inform all peers, its fine if the message doesn't reach them as those peers would eventually learn during their paxos phases
	for i := 0; i < peersCnt; i++ {
		if i == px.me {
			px.UpdateDecided(request, &response)
		} else {
			call(px.peers[i], "Paxos.UpdateDecided", *request, &response)
		}
	}
}

func (px *Paxos) UpdateDecided(req *Request, res *Response) error {
	px.mu.Lock()
	status, exist := px.seqStatus[req.SequenceNumber]

	// Make the status of this sequence Decided and note the value
	if exist == true {
		status.currentRequest.ProposalValue = req.ProposalValue
	} else {
		status = Status{Decided, req.ProposalNumber, *req}
	}

	status.Fate = Decided
	px.seqStatus[req.SequenceNumber] = status

	res.Promise = 0
	px.mu.Unlock()
	return nil
}

func (px *Paxos) Forget(minSeq int) {
	// minSeq is smallest_done sequence + 1 as known to this host
	// So we should be safe to delete any seqStatus for seqNumber < minSeq
	px.mu.Lock()
	for key, _ := range px.seqStatus {
		if key < minSeq {
			delete(px.seqStatus, key)
		}
	}
	px.mu.Unlock()
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	minSeq := px.Min()
	if seq < minSeq {
		//ignore this Start call
		return
	}

	// Update max sequence seen
	if seq > px.maxseq {
		px.maxseq = seq
	}

	go func() {
		for true {
			// Checking if the connection is dead or not, if so we break out of the loop
			if px.isdead() {
				return
			}

			px.mu.Lock()
			// Update this peers proposal number for future requests
			newProposalNumber := px.proposalNumber
			px.proposalNumber = px.proposalNumber + len(px.peers)

			prepareRequest := Request{seq, newProposalNumber, v}
			px.mu.Unlock()

			// Adds a random sleep to prevent livelock
			rand.Seed(time.Now().UnixNano())
			randomSleep := time.Duration(rand.Intn(10)) * time.Millisecond
			time.Sleep(randomSleep)

			// Prepare phase starts
			status, val := px.SendPrepare(&prepareRequest)
			if status == false {
				// Prepare failed, try again
				continue
			}

			px.Forget(px.Min())

			acceptRequest := Request{seq, newProposalNumber, val}

			// Accept phase starts
			status = px.SendAccept(&acceptRequest)
			if status == false {
				// Accept failed, try again from prepare
				continue
			}

			if px.isdead() {
				return
			}

			// Try to notify other peers that value has been decided
			px.InformDecided(&acceptRequest)

			// If we get this far, its safe to assume paxos is done for this peer
			return
		}
	}()

	return
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {

	//Only update done if seq is higher than the last done sequence
	px.mu.Lock()
	currDone, _ := px.doneTracker[px.me]
	if seq > currDone {
		px.doneTracker[px.me] = seq
	}
	px.mu.Unlock()
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	maxSeq := px.maxseq
	px.mu.Unlock()

	return maxSeq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	min := MAX_VALUE
	for i := 0; i < len(px.peers); i++ {
		value, _ := px.doneTracker[i]
		if value < min {
			min = value
		}
	}
	px.mu.Unlock()
	return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {

	if seq < px.Min() {
		return Forgotten, nil
	}

	px.mu.Lock()
	status, ok := px.seqStatus[seq]
	px.mu.Unlock()

	if ok {
		return status.Fate, status.currentRequest.ProposalValue
	}
	return Pending, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.seqStatus = map[int]Status{}
	px.doneTracker = map[int]int{}

	// initially no peer will call Done
	for i := 0; i < len(px.peers); i++ {
		px.doneTracker[i] = -1
	}
	px.proposalNumber = px.me
	px.maxseq = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
