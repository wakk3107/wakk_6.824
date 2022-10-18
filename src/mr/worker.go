package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := CallGetTask(&args, &reply)
		log.Printf("recv get task reply: %v\n", reply)
		//没任务了就关机
		if !ok || reply.Type == STOP {
			break
		}

		// handle map fynction
		switch reply.Type {
		case MAP:
			if len(reply.Filenames) < 1 {
				log.Fatalf("don't have filename")
			}
			DoMAP(reply.Filenames[0], reply.TaskId, reply.NReduce, mapf)
			// map complete, send msg to master
			finish_args := FinishTaskArgs{
				Type:   MAP,
				TaskId: reply.TaskId,
			}
			finish_reply := FinishTaskReply{}
			log.Printf("finish request: %v\n", finish_args)
			CallFinishTask(&finish_args, &finish_reply)
			log.Printf("recv finish reply: %v\n", finish_reply)
			// time.Sleep(time.Second)
		case REDUCE:
			if len(reply.Filenames) < 1 {
				log.Fatalf("don't have filenames")
			}
			DoReduce(reply.Filenames, reply.TaskId, reducef)
			// reduce complete, send msg to master
			finish_args := FinishTaskArgs{
				Type:   REDUCE,
				TaskId: reply.TaskId,
			}
			finish_reply := FinishTaskReply{}
			log.Printf("finish request: %v\n", finish_args)
			CallFinishTask(&finish_args, &finish_reply)
			log.Printf("recv finish reply: %v\n", finish_reply)
			// time.Sleep(time.Second)
		case WAIT:
			log.Printf("wait task\n")
			time.Sleep(time.Second)
		default:
			time.Sleep(time.Second)
		}
	}
}

func DoMAP(filename string, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//返回 KV 数组
	kva := mapf(filename, string(content))

	sort.Sort(ByKey(kva))

	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		//生成临时文件来放结果
		ofile, err := ioutil.TempFile("", "mr-tmp*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		defer ofile.Close()

		encoder := json.NewEncoder(ofile)
		encoders[i] = encoder
		files[i] = ofile
	}

	var index int
	for _, kv := range kva {
		//根据 key 不同写入文件中
		index = ihash(kv.Key) % nReduce
		err = encoders[index].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}

	// 重命名临时文件
	for i := 0; i < nReduce; i++ {
		filename_tmp := fmt.Sprintf("mr-%d-%d", taskId, i)
		err := os.Rename(files[i].Name(), filename_tmp)
		if err != nil {
			log.Fatalf("cannot rename %v to %v", files[i].Name(), filename_tmp)
		}
	}
}

func DoReduce(filenames []string, taskId int, reducef func(string, []string) string) {
	// read data from mid-file
	kva := make([]KeyValue, 0)
	//取出所有文件中的单词都放入一个数组中
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	//排序
	sort.Sort(ByKey(kva))

	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	ofile, err := ioutil.TempFile("", "mr-out-tmp*")
	if err != nil {
		log.Fatalf("cannot create temp file")
	}
	defer ofile.Close()

	i := 0 //计算与第 i 个单词相同的单词数量
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		//其实就是 strconv.Itoa(len(values)),在 wc.go 里
		//单词数量
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	output_filename := fmt.Sprintf("mr-out-%d", taskId)
	err = os.Rename(ofile.Name(), output_filename)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", ofile.Name(), output_filename)
	}
}

// rpc interface
func CallGetTask(args *GetTaskArgs, reply *GetTaskReply) bool {
	// send the RPC request, wait for the reply.
	return call("Coordinator.GetTask", args, reply)
}

func CallFinishTask(args *FinishTaskArgs, reply *FinishTaskReply) bool {
	return call("Coordinator.FinishTask", args, reply)
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
		log.Println("dialing: ", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
