package kvpaxos

import (
	"cse-513/src/paxos"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkUniqueNumber int
	Operation         string
	Key               string
	Value             string
	ClerkId           int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	sequenceNumber   int               // Sequence number for paxos protocol
	kvStore          map[string]string // The key-value Store
	clerkDoneTracker map[int64]int     // Track the highest request served for given clerk
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()

	// If we have already served a request with higher ClerkSequenceNumber
	// This means we have relatively latest value, so directly return it
	if args.ClerkSequenceNumber <= kv.clerkDoneTracker[args.ClerkId] {
		reply.Value = kv.kvStore[args.Key]
		kv.mu.Unlock()
		return nil
	}

	for {
		seq := kv.sequenceNumber
		kv.sequenceNumber += 1

		op := Op{args.ClerkSequenceNumber, "Get", args.Key, "", args.ClerkId}

		// Start a paxos instance for the requested operation
		kv.px.Start(seq, op)

		tts := 10 * time.Millisecond
		for {

			// Sleep for some time, for paxos to finish deciding
			time.Sleep(tts)
			if tts < 10*time.Second {
				tts *= 2
			}

			status, returnedOp := kv.px.Status(seq)

			// This server can learn about operations that have might have been done for this sequenceNumber
			if status == paxos.Decided {
				if returnedOp.(Op).Operation != "Get" { // Put/Append operation
					kv.PerformPutAppend(returnedOp.(Op))
				}

				if returnedOp.(Op).ClerkUniqueNumber > kv.clerkDoneTracker[returnedOp.(Op).ClerkId] {
					kv.clerkDoneTracker[returnedOp.(Op).ClerkId] = returnedOp.(Op).ClerkUniqueNumber
				}

				// If operation decided for this sequence is same as the requested Operation
				// Return the stored value
				if kv.isEqual(op, returnedOp.(Op)) {
					reply.Value = kv.kvStore[returnedOp.(Op).Key]
					kv.mu.Unlock()
					kv.px.Done(seq)// Mark this sequenceNumber done
					return nil
				}
				break
			}
		}
	}
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()

	// If we have already served a request with higher ClerkSequenceNumber
	// This means we have relatively latest value, so directly return it
	if args.ClerkSequenceNumber <= kv.clerkDoneTracker[args.ClerkId] {
		kv.mu.Unlock()
		return nil
	}

	for {
		seq := kv.sequenceNumber
		kv.sequenceNumber += 1

		op := Op{args.ClerkSequenceNumber, args.Op, args.Key, args.Value, args.ClerkId}

		// Start a paxos instance for the requested operation
		kv.px.Start(seq, op)

		tts := 10 * time.Millisecond

		for {
			// Sleep for some time, for paxos to finish deciding
			time.Sleep(tts)
			if tts < 10*time.Second {
				tts *= 2
			}

			status, returnedOp := kv.px.Status(seq)

			// This server can learn about operations that have might have been done for this sequenceNumber
			if status == paxos.Decided {
				if returnedOp.(Op).Operation != "Get" {
					kv.PerformPutAppend(returnedOp.(Op))
				}

				if returnedOp.(Op).ClerkUniqueNumber > kv.clerkDoneTracker[returnedOp.(Op).ClerkId] {
					kv.clerkDoneTracker[returnedOp.(Op).ClerkId] = returnedOp.(Op).ClerkUniqueNumber
				}

				// If operation decided for this sequence is same as the requested Operation
				// Return, as we have performed this operation above
				if kv.isEqual(op, returnedOp.(Op)) {
					kv.mu.Unlock()
					kv.px.Done(seq)// // Mark this sequenceNumber done
					return nil
				}
				break
			}
		}
	}
}

func (kv *KVPaxos) isEqual(op1 Op, op2 Op) bool {
	return op1.ClerkUniqueNumber == op2.ClerkUniqueNumber &&
		op1.Operation == op2.Operation &&
		op1.Key == op2.Key &&
		op1.Value == op2.Value &&
		op1.ClerkId == op2.ClerkId
}

func (kv *KVPaxos) PerformPutAppend(op Op) {
	// Your code here.

	// Update values, only if its latest
	if kv.clerkDoneTracker[op.ClerkId] >= op.ClerkUniqueNumber {
		return
	}
	switch op.Operation {
	case "Put":
		kv.kvStore[op.Key] = op.Value
		break
	case "Append":
		_, exists := kv.kvStore[op.Key]
		if !exists {
			kv.kvStore[op.Key] = ""
		}
		kv.kvStore[op.Key] += op.Value
		break
	default:
		fmt.Println("Invalid operation")
		break
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.kvStore = map[string]string{}
	kv.sequenceNumber = 0
	kv.clerkDoneTracker = map[int64]int{}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
