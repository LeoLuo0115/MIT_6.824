package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	// fmt.Printf("I am worker")
	// RPC call to get a task from coordinator
	for {
		reply := CallTask()
		
		switch reply.Task.TaskType {
		case Map:
			handleMapTask(reply.Task, mapf)
			time.Sleep(1 * time.Second)
		case Reduce:
			handleReduceTask(reply.Task, reducef)
		case Wait:
			time.Sleep(3 * time.Second)
		case Terminate:
			return
		}
	}
	
}

func CallTask() TaskReply {
	// declare an argument structure.
	args := TaskArgs{}
	// fill in the argument(s).
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// fmt.Printf("TaskType is: %v\n", reply.Task.Type)
		fmt.Printf("TaskArgs: %+v\n", reply.Task)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply
}

func handleMapTask (task Task, mapf func(string, string) []KeyValue) {
	// 执行 map task
}
func handleReduceTask(task Task, reducef func(string, []string) string) {
	
}


//
// send an RPC request to the coordinator, wait for the replyonse.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
