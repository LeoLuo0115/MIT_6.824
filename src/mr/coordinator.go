package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type Status int

const (
    InProgress = iota
    Completed
	Waited
)


type TaskType int

const (
    Map = iota
    Reduce 
    Wait   // there is no task right now
    Terminate    // all task is finished, worker need to close itse
)

func IsValidType(t TaskType) bool {
    switch t {
    case Map, Reduce, Wait, Terminate:
        return true
    default:
        return false
    }
}

type Task struct {
	Filename string
	TaskNum int
	TaskType TaskType
	Status Status
}


func createTask(filename string, taskNum int, taskType TaskType, status Status) Task {
	return Task{
		Filename: filename,
		TaskType: taskType,
		TaskNum:  taskNum,
		Status: status,
	}
}

type phase int

const (
	MapPhase = iota
	ReducePhase
	Finished
)


type Coordinator struct {
	nreduce int
	TaskArray []Task
	ReduceTaskArgs []Task
	mu       sync.Mutex
	nextTask int
	MapRetransmission_queue  MyCircularQueue
	ReduceRetransmission_queue  MyCircularQueue
	phase phase
}

func (c *Coordinator) monitorTask(task *Task) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    select {
    case <-ticker.C:
        if task.Status != Completed {
            c.MapRetransmission_queue.EnQueue(task.TaskNum)
            task.Status = Waited
        }
    default:
        // 任务未在10秒内完成，你可以在这里添加处理代码
    }
}

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {	
    c.mu.Lock()
    defer c.mu.Unlock()
	
	
	// 分配任务或nil，nil 表示暂无可分配的任务
	switch c.phase {
	case MapPhase:
		task := c.findMapTask()
		reply.Task = task
		// go c.monitorTask(&task)
	case ReducePhase:
		reply.Task = c.findReduceTask()
	case Finished:
		reply.Task.TaskType = Terminate
	}
	
	
    return nil
}

func (c *Coordinator) findMapTask() Task {
    var task Task
    if c.nextTask < len(c.TaskArray) {
        // 如果还有未分配的任务，取一个
        task = c.TaskArray[c.nextTask]
        task.Status = InProgress
        c.nextTask++
    } else if c.MapRetransmission_queue.Size() > 0 {
        // 如果有需要重新分配的任务，取一个
        e := c.MapRetransmission_queue.Front()
        c.MapRetransmission_queue.DeQueue()
        task = c.TaskArray[e]
        task.Status = InProgress
    } else {
        // 否则，返回一个空任务
        task = Task{TaskType: Wait}
    }
    return task
}

func (c *Coordinator) findReduceTask() Task {
    var task Task
    if c.nextTask < len(c.ReduceTaskArgs) {
        // 如果还有未分配的任务，取一个
        task = c.ReduceTaskArgs[c.nextTask]
		task.Status = InProgress
        c.nextTask++	
    } else if c.ReduceRetransmission_queue.Size() > 0 {
        // 如果有需要重新分配的任务，取一个
        e := c.ReduceRetransmission_queue.Front()
        c.ReduceRetransmission_queue.DeQueue()
        task = c.ReduceTaskArgs[e]
		task.Status = InProgress
    } else {
        // 否则，返回一个空任务
        task = Task{TaskType: Wait}
    }
    return task
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
	// set phase to MapPhase
	c.phase = MapPhase
	// set nReduce
	c.nreduce = nReduce
	// init retransmission_queue
	c.MapRetransmission_queue = MQConstructor(len(files))
	// read input files to TaskArray
	num := 0
	for _, filename := range files {
		mapTask := createTask(filename, num, Map, Waited)
		num++
		c.TaskArray = append(c.TaskArray, mapTask)
	}
	
	// print TaskArray
	for _, e:= range c.TaskArray {
		fmt.Printf("TaskArray: %+v\n", e)
	}
	
	// call server to listen for RPCs from worker
	c.server()
	return &c
}
