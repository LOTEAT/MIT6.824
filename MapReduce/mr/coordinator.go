package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// some const variables
const (
	schedule_interval = time.Millisecond * 500
	MaxRunningTime    = time.Second * 5
)

// coordinator phase : map & reduce
type CoPhase int

const (
	CoMap    CoPhase = 0
	CoReduce CoPhase = 1
)

// task phase
type TaskPhase int

const (
	TaskUnallocated TaskPhase = 0
	TaskReady       TaskPhase = 1
	TaskRunning     TaskPhase = 2
	TaskDone        TaskPhase = 3
	TaskError       TaskPhase = 4
)

type Task struct {
	FileName string
	Idx      int
	Phase    CoPhase
	NReduce  int
	NMap     int
	IsAlive  bool
}

type TaskState struct {
	WorkerId  int
	StartTime time.Time
	Phase     TaskPhase
}

type Coordinator struct {
	files       []string
	nReduce     int
	phase       CoPhase
	task_state  []TaskState
	tasks       chan Task
	worker_nums int
	is_done     bool
	lock        sync.Mutex
}

//
// create a new task
//
func (c *Coordinator) create_task(file_idx int) Task {
	var file_name string
	if c.phase == CoMap {
		file_name = c.files[file_idx]
	} else {
		file_name = ""
	}
	task := Task{
		FileName: file_name,
		Idx:      file_idx,
		NMap:     len(c.files),
		Phase:    c.phase,
		NReduce:  c.nReduce,
		IsAlive:  true,
	}
	return task
}


func (c *Coordinator) allocate_task() {
	if c.is_done {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()
	allDone := true
	for k, v := range c.task_state {
		switch v.Phase {
		case TaskUnallocated:
			allDone = false
			c.task_state[k].Phase = TaskReady
			c.tasks <- c.create_task(k)
		case TaskReady:
			allDone = false
		case TaskRunning:
			allDone = false
			if time.Now().Sub(v.StartTime) > MaxRunningTime {
				c.task_state[k].Phase = TaskReady
				c.tasks <- c.create_task(k)
			}
		case TaskDone:
		case TaskError:
			allDone = false
			c.task_state[k].Phase = TaskReady
			c.tasks <- c.create_task(k)
		default:
			panic("t. status err in schedule")
		}
	}

	if allDone {
		if c.phase == CoMap {
			c.phase = CoReduce
			c.task_state = make([]TaskState, c.nReduce)
		} else {
			log.Println("finish all tasks!")
			c.is_done = true
		}
	}
}


func (c *Coordinator) schedule() {
	for !c.Done() {
		c.allocate_task()
		time.Sleep(schedule_interval)
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	task := <-c.tasks
	reply.Task = &task
	if task.IsAlive {
		c.lock.Lock()
		defer c.lock.Unlock()
		if task.Phase != c.phase {
			return errors.New("Wrong task phase!")
		}
		c.task_state[task.Idx].Phase = TaskRunning
		c.task_state[task.Idx].StartTime = time.Now()
		c.task_state[task.Idx].WorkerId = args.WorkerId
	}
	return nil
}

func (c *Coordinator) RegisterWorker(args *WorkerRegisterArgs, reply *WorkerRegisterReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.worker_nums++
	reply.WorkerId = c.worker_nums
	return nil
}

func (c *Coordinator) ReportTaskState(args *TaskReportArgs, reply *TaskReportReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.phase != args.Phase || c.task_state[args.Idx].WorkerId != args.WorkerId {
		return nil
	}

	if args.IsDone {
		c.task_state[args.Idx].Phase = TaskDone
	} else {
		c.task_state[args.Idx].Phase = TaskError
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)  // 注册 RPC 服务
	rpc.HandleHTTP() // 将 RPC 服务绑定到 HTTP 服务中去
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.is_done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		phase:       CoMap,
		task_state:  make([]TaskState, len(files)),
		worker_nums: 0,
		is_done:     false,
	}
	if len(files) > nReduce {
		c.tasks = make(chan Task, len(files))
	} else {
		c.tasks = make(chan Task, nReduce)
	}

	go c.schedule()
	c.server()

	return &c
}
