package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	FileNum               int
	ReduceNum             int
	FileName              []string
	MapTask               []int //0 not allocated, 1 doing,2 done
	ReduceTask            []int
	MapTaskFinishedNum    int
	ReduceTaskFinishedNum int
	lock                  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) CreateTask(args * RequestArgs, replyArgs * ReplyArgs) error {
	c.lock.Lock()
	if c.MapTaskFinishedNum < c.FileNum {
		index := -1
		for i, value := range c.MapTask {
			if value == 0 {
				index = i
				break
			}
		}
		if index == -1 {
			log.Println("no map task needs to be done")
			replyArgs.TaskType = 2
			c.lock.Unlock()
			return nil
		}
		c.MapTask[index] = 1
		c.lock.Unlock()
		replyArgs.MapTaskId = index
		replyArgs.MapTaskNum = c.FileNum
		replyArgs.FileName = c.FileName[index]
		replyArgs.ReduceTaskNum = c.ReduceNum
		replyArgs.TaskType = 0
		go func() {
			time.Sleep(time.Second * 10)
			c.lock.Lock()
			if c.MapTask[index] == 1 {
				c.MapTask[index] = 0
			}
			c.lock.Unlock()

		}()
	} else if c.ReduceTaskFinishedNum < c.ReduceNum {
		index := -1
		for i, value := range c.ReduceTask {
			if value == 0 {
				index = i
				break
			}
		}
		if index == -1 {
			log.Println("no reduce task needs to be done")
			replyArgs.TaskType = 2
			c.lock.Unlock()
			return nil
		}
		c.ReduceTask[index] = 1
		c.lock.Unlock()
		replyArgs.ReduceTaskId = index
		replyArgs.ReduceTaskNum = c.ReduceNum
		replyArgs.MapTaskNum = c.FileNum
		replyArgs.TaskType = 1
		go func() {
			time.Sleep(time.Second * 10)
			c.lock.Lock()
			if c.ReduceTask[index] == 1 {
				c.ReduceTask[index] = 0
			}
			c.lock.Unlock()

		}()

	} else {
		replyArgs.TaskType = 3
	}
	return nil
}
func (c *Coordinator) MapTaskDone(args *RequestArgs, replyArgs *ReplyArgs) error {
	fmt.Println("maptask done")
	c.lock.Lock()
	c.MapTask[args.MapTaskId] = 2
	c.MapTaskFinishedNum++
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) ReduceTaskDone(args *RequestArgs, replyArgs *ReplyArgs) error {
	c.lock.Lock()
	c.ReduceTask[args.ReduceTaskId] = 2
	c.ReduceTaskFinishedNum++
	c.lock.Unlock()
	fmt.Println("reduce task",c.ReduceTask)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := c.ReduceTaskFinishedNum == c.ReduceNum

	// Your code here.

	return ret
}



// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.FileName = files
	c.FileNum = len(files)
	c.ReduceNum = nReduce
	c.MapTask = make([]int, c.FileNum)
	c.ReduceTask = make([]int, c.ReduceNum)
	c.MapTaskFinishedNum = 0
	c.ReduceTaskFinishedNum = 0
	c.server()
	return &c
}
