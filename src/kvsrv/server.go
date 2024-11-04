package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu              sync.Mutex
	kvMap           map[string]string
	requestTracker  map[int64]int64  // clientID -> requestID
	responseTracker map[int64]string // clientID -> response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kvMap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if lastRequest, exists := kv.requestTracker[args.ClientId]; exists {
		if args.RequestId <= lastRequest {
			return
		}
	}
	kv.kvMap[args.Key] = args.Value
	reply.Value = args.Value
	kv.requestTracker[args.ClientId] = args.RequestId

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if lastRequest, exists := kv.requestTracker[args.ClientId]; exists {
		if args.RequestId <= lastRequest {
			reply.Value = kv.responseTracker[args.ClientId]
			return
		}
	}

	preAppendValue := kv.kvMap[args.Key]
	kv.kvMap[args.Key] += args.Value
	reply.Value = preAppendValue

	kv.requestTracker[args.ClientId] = args.RequestId
	kv.responseTracker[args.ClientId] = preAppendValue
}

func StartKVServer() *KVServer {
	/*
		kv := new(KVServer)
		kv.mu = sync.Mutex{}
		kv.kvMap = make(map[string]string)
		kv.serverAddr = "127.0.0.1:0"

		rpc.Register(kv)
		listener, err := net.Listen("tcp", kv.serverAddr)
		if err != nil {
			log.Fatalf("Failed to listen on %s: %v", kv.serverAddr, err)
		}

		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Printf("Failed to accept connection: %v", err)
					continue
				}
				go rpc.ServeConn(conn)
			}
		}()
		return kv
	*/
	kv := new(KVServer)
	kv.mu = sync.Mutex{}
	kv.kvMap = make(map[string]string)
	kv.requestTracker = make(map[int64]int64)
	kv.responseTracker = make(map[int64]string)
	return kv
}
