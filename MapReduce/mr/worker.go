package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Machine struct {
	worker_id int
	mapf      func(string, string) []KeyValue
	reducef   func(string, []string) string
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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	worker := Machine{
		mapf:    mapf,
		reducef: reducef,
	}
	worker.register()
	worker.run()
}

func (m *Machine) run() {
	for {
		task, err := m.get_task()
		if err != nil {
			continue
		}
		if !task.IsAlive {
			return
		}
		m.do_task(*task)
	}
}

func (m *Machine) do_task(task Task) {
	if task.Phase == CoMap {
		m.do_map(task)
	} else if task.Phase == CoReduce {
		m.do_reduce(task)
	} else {
		panic("Something went wrong!")
	}
}

func (m *Machine) map_file_name(task_id, pat int) string {
	return fmt.Sprintf("mr-%d-%d", task_id, pat)
}

func (m *Machine) reduce_file_name(pat int) string {
	return fmt.Sprintf("mr-out-%d", pat)
}

func (m *Machine) do_map(task Task) {
	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		fmt.Println(err)
		m.report(task, false)
	}
	results := m.mapf(task.FileName, string(content))
	partitions := make([][]KeyValue, task.NReduce)
	for _, kv := range results {
		pat := ihash(kv.Key) % task.NReduce
		partitions[pat] = append(partitions[pat], kv)
	}
	for row, kvs := range partitions {
		temp_file_name := m.map_file_name(task.Idx, row)
		file, err := os.Create(temp_file_name)
		if err != nil {
			m.report(task, false)
			return
		}
		encoder := json.NewEncoder(file)
		for _, kv := range kvs {
			if err := encoder.Encode(&kv); err != nil {
				m.report(task, false)
				// return
			}
		}
		if err := file.Close(); err != nil {
			m.report(task, false)
		}
	}
	m.report(task, true)
}

func (m *Machine) do_reduce(task Task) {
	kvs_merge := make(map[string][]string)
	for i := 0; i < task.NMap; i++ {
		file_name := m.map_file_name(i, task.Idx)
		file, err := os.Open(file_name)
		if err != nil {
			m.report(task, false)
			return
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := kvs_merge[kv.Key]; !ok {
				kvs_merge[kv.Key] = make([]string, 0)
			}
			kvs_merge[kv.Key] = append(kvs_merge[kv.Key], kv.Value)
		}
	}

	results := make([]string, 0)
	for k, v := range kvs_merge {
		cnt := m.reducef(k, v)
		results = append(results, fmt.Sprintf("%v %v\n", k, cnt))
	}

	file_name := m.reduce_file_name(task.Idx)
	if err := ioutil.WriteFile(file_name, []byte(strings.Join(results, "")), 0600); err != nil {
		m.report(task, false)
	}

	m.report(task, true)
}

func (m *Machine) register() {
	args := WorkerRegisterArgs{}
	reply := WorkerRegisterReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		m.worker_id = reply.WorkerId
		fmt.Printf("Register successfully! The worker id is %v\n", reply.WorkerId)

	} else {
		fmt.Printf("Register failed!\n")
	}
}

func (m *Machine) get_task() (*Task, error) {
	args := TaskArgs{WorkerId: m.worker_id}
	reply := TaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		fmt.Printf("Get task successfully!\n")
		return reply.Task, nil
	} else {
		fmt.Printf("Get task failed!\n")
		return nil, errors.New("Task Error!")
	}
}

func (m *Machine) report(task Task, is_done bool) {
	args := TaskReportArgs{
		WorkerId: m.worker_id,
		Phase:    task.Phase,
		Idx:      task.Idx,
		IsDone:   is_done,
	}
	reply := TaskReportReply{}
	ok := call("Coordinator.ReportTaskState", &args, &reply)
	if ok {
		fmt.Printf("Report successfully!\n")
	} else {
		fmt.Printf("Report failed!\n")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
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
