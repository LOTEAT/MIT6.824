# MIT6.824

## 1. coordinator

### 1.1 const value

```go
// some const variables
const (
	schedule_interval = time.Millisecond * 500
	MaxRunningTime    = time.Second * 5
)
```
`schedule_interval` is used for setting scheduling interval, and `MaxRunningTime` is used for limiting the time of program running.

### 1.2 enum value
```go
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
```
CoMap: The program is in the `Map` phase.

CoReduce: The program is in the `Reduce` phase.

### 1.3 struct value
```go
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
```
### 1.4 function
#### 1.4.1 create_task
```go
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
```
`create_task` is used to create a task. Note that the phase of coordinator is important. `IsAlive` will be used to judge whether the task is done.

#### 1.4.2 allocate_task
```go
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
```
This is like state machine. What state the task should be is determined by its current state.

#### 1.4.3 schedule
```go
func (c *Coordinator) schedule() {
	for !c.Done() {
		c.allocate_task()
		time.Sleep(schedule_interval)
	}
}
```

#### 1.4.4 RPC functions
```go
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
```
It is easy to understand.

## 2. RPC
```go
type TaskArgs struct {
	WorkerId int
}
type TaskReply struct {
	Task *Task
}

type WorkerRegisterArgs struct {
}
type WorkerRegisterReply struct {
	WorkerId int
}

type TaskReportArgs struct {
	WorkerId int
	Phase    CoPhase
	Idx      int
	IsDone   bool
}
type TaskReportReply struct {
}
```
very easy.

## 3. Worker
Please refer to the code implementation