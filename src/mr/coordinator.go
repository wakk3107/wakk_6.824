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

//当前任务状态
type TaskStatus int

const (
	idle        TaskStatus = iota //空闲
	in_progress                   //已分配
	completed                     //已完成
)

type Task struct {
	taskId    int        //task id
	filenames []string   //被分配的任务文件
	status    TaskStatus //当前任务状态
	startTime time.Time  //开始时间，根据这个判断是否下线
}

//协调者的状态
type CoordinatorStatus int

const (
	MAP_PHASE    CoordinatorStatus = iota //处于map阶段
	REDUCE_PHASE                          //处于reduce阶段
	FINISH_PHASE                          //全完成阶段
)

type Coordinator struct {
	// Your definitions here.
	tasks   []Task            //所拥有的任务
	nReduce int               //reduce 工作的数量
	nMap    int               //map 工作的数量
	status  CoordinatorStatus ////协调者的状态
	mu      sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	//检查一下当前阶段任务是否全都完成，若都已完成，切换到下个阶段继续分配
	finish_flag := c.IsAllFinish()
	if finish_flag {
		c.NextPhase()
	}
	for i := 0; i < len(c.tasks); i++ {
		//有闲置任务则分配
		if c.tasks[i].status == idle {
			log.Printf("send task %d to worker\n", i)
			reply.Err = SuccessCode
			reply.TaskId = i
			reply.Filenames = c.tasks[i].filenames
			if c.status == MAP_PHASE {
				reply.Type = MAP
				reply.NReduce = c.nReduce
			} else if c.status == REDUCE_PHASE {
				reply.NReduce = 0
				reply.Type = REDUCE
			} else {
				log.Fatal("unexpected status")
			}
			//初始化开始时间，若超时，则重新分配任务
			c.tasks[i].startTime = time.Now()
			c.tasks[i].status = in_progress
			return nil

		} else if c.tasks[i].status == in_progress {
			//无闲置任务则分配超时任务
			curr := time.Now()
			if curr.Sub(c.tasks[i].startTime) > time.Second*10 {
				log.Printf("resend task %d to worker\n", i)
				reply.Err = SuccessCode
				reply.TaskId = i
				reply.Filenames = c.tasks[i].filenames
				if c.status == MAP_PHASE {
					reply.Type = MAP
					reply.NReduce = c.nReduce
				} else if c.status == REDUCE_PHASE {
					reply.NReduce = 0
					reply.Type = REDUCE
				} else {
					log.Fatal("unexpected status")
				}
				c.tasks[i].startTime = time.Now()
				return nil
			}
		}
	}
	reply.Err = SuccessCode
	reply.Type = WAIT
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskId >= len(c.tasks) || args.TaskId < 0 {
		reply.Err = ParaErrCode
		return nil
	}
	c.tasks[args.TaskId].status = completed
	if c.IsAllFinish() {
		c.NextPhase()
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

// coordinator init code
func (c *Coordinator) Init(files []string, nReduce int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("init coordinator")

	// 初始化 map 任务
	log.Println("make map tasks")
	tasks := make([]Task, len(files))
	for i, file := range files {
		tasks[i].taskId = i
		tasks[i].filenames = []string{file}
		tasks[i].status = idle
	}

	// init coordinator
	c.tasks = tasks
	c.nReduce = nReduce
	c.nMap = len(files)
	c.status = MAP_PHASE
}

func (c *Coordinator) MakeReduceTasks() {
	// make reduce tasks
	log.Println("make reduce tasks")
	tasks := make([]Task, c.nReduce)
	// 初始化 reduce 任务
	for i := 0; i < c.nReduce; i++ {
		tasks[i].taskId = i
		files := make([]string, c.nMap)
		for j := 0; j < c.nMap; j++ {
			filename := fmt.Sprintf("mr-%d-%d", j, i)
			files[j] = filename
		}
		tasks[i].filenames = files
		tasks[i].status = idle
	}
	c.tasks = tasks
}

func (c *Coordinator) IsAllFinish() bool {
	for i := len(c.tasks) - 1; i >= 0; i-- {
		if c.tasks[i].status != completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) NextPhase() {
	if c.status == MAP_PHASE {
		log.Println("change to REDUCE_PHASE")
		c.MakeReduceTasks()
		c.status = REDUCE_PHASE
	} else if c.status == REDUCE_PHASE {
		log.Println("change to FINISH_PHASE")
		c.status = FINISH_PHASE
	} else {
		log.Println("unexpected status change!")
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status == FINISH_PHASE {
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.Init(files, nReduce)
	//等待 worker 来要任务
	c.server()
	return &c
}
