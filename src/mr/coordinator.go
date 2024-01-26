package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Type string

const (
    Map    Type = "map"
    Reduce Type = "reduce"
    Null   Type = "null"
    End    Type = "end"
)

func IsValidType(t Type) bool {
    switch t {
    case Map, Reduce, Null, End:
        return true
    default:
        return false
    }
}

type MapTask struct {
	Filename string
	TaskNum int
	Type Type
}


func createTask(filename string, taskNum int, Type Type) MapTask {
	return MapTask{
		Filename: filename,
		Type: Type,
		TaskNum:  taskNum,
	}
}


type Coordinator struct {
	nreduce int
	TaskArgs []MapTask
	mu       sync.Mutex
	nextTask int
	retransmission_queue  []int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *MapTaskArgs, reply *MapTaskReply) error {	
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.Done() {
        reply.Task.Type = End
        return nil
    }
    
    if c.nextTask < len(c.TaskArgs) {
        e := c.TaskArgs[c.nextTask]
        reply.Task = e
        c.nextTask++
    } else if len(c.retransmission_queue) > 0 {
        for _, e := range c.retransmission_queue {
            reply.Task = c.TaskArgs[e]
        }
    } else {
        reply.Task.Type = Null
    }

    return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// create a Coordinator.
	c := Coordinator{}
	// set nReduce
	c.nreduce = nReduce
	
	// read input files to TaskArgs
	num := 0
	for _, filename := range files {
		mapTask := createTask(filename, num, Map)
		num++
		c.TaskArgs = append(c.TaskArgs, mapTask)
	}
	
	// print TaskArgs
	for _, e:= range c.TaskArgs {
		fmt.Printf("TaskArgs: %+v\n", e)
	}
	
	// call server to listen for RPCs from worker
	c.server()
	return &c
}
