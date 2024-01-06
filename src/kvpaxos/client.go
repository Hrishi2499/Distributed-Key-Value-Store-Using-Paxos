package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	ClerkId           int64 // Unique ID of this clerk
	ClerkUniqueNumber int   // Unique Number to track requests and deal with duplicates
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClerkId = nrand()
	ck.ClerkUniqueNumber = 1
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

//


// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	serverCount := len(ck.servers)
	serverId := 0
	// fetch the current value for a key.
	req := &GetArgs{key, ck.ClerkId, ck.ClerkUniqueNumber}
	ck.ClerkUniqueNumber += 1
	res := &GetReply{}

	for {
		success := call(ck.servers[serverId], "KVPaxos.Get", req, res)
		if success == true {
			break
		}

		// If call was not successful, try next server (first after last)
		serverId = (serverId + 1) % serverCount
	}
	return res.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	serverCount := len(ck.servers)
	serverId := 0

	req := &PutAppendArgs{key, value, op, ck.ClerkId, ck.ClerkUniqueNumber}
	ck.ClerkUniqueNumber += 1
	res := &PutAppendReply{}

	for {
		success := call(ck.servers[serverId], "KVPaxos.PutAppend", req, res)
		if success == true {
			break
		}

		// If call was not successful, try next server (first after last)
		serverId = (serverId + 1) % serverCount
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
