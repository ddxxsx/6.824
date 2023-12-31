package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()
	for {
		argc := RequestArgs{}
		reply := ReplyArgs{}
		call("Coordinator.CreateTask", &argc, &reply)
		if reply.TaskType == 2 {
			time.Sleep(time.Second * 5)
		} else if reply.TaskType == 3 {
			break
		}

		if reply.TaskType == 0 {
			hash_table := make([][]KeyValue, reply.ReduceTaskNum)
			content, err := ioutil.ReadFile(reply.FileName)
			if err != nil {
				fmt.Println("open file error ", reply.FileName)
			}
			for i := range hash_table {
				hash_table[i] = []KeyValue{}
			}
			kv := mapf(reply.FileName, string(content))
			for _, j := range kv {
				hash_table[ihash(j.Key)%reply.ReduceTaskNum] = append(hash_table[ihash(j.Key)%reply.ReduceTaskNum], j)
			}
			for i := range hash_table {

				Tempfile := "mr-" + strconv.Itoa(reply.MapTaskId) + "-" + strconv.Itoa(i)
				file, err := os.Create(Tempfile)
				if err!=nil{
					log.Fatalf("error",err.Error())
					return
				}
				encoder := json.NewEncoder(file)
				for _, value := range hash_table[i] {
					err := encoder.Encode(value)
					if err != nil {
						log.Println("error Json Trans")
					}
				}
			}
			req := RequestArgs{reply.MapTaskId, -1}
			rep := ReplyArgs{}
			call("Coordinator.MapTaskDone", &req, &rep)
		} else if reply.TaskType == 1 {
			Content := []KeyValue{}
			for i := 0; i < reply.MapTaskNum; i++ {
				FileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskId)
				file, err := os.Open(FileName)
				if err != nil {
					log.Println("open file failed", FileName)
					return
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					Content = append(Content, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(Content))

			// output file
			oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskId)
			ofile, _ := ioutil.TempFile("", oname+"*")
			i := 0
			for i < len(Content) {
				j := i + 1
				for j < len(Content) && Content[j].Key == Content[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, Content[k].Value)
				}
				output := reducef(Content[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", Content[i].Key, output)

				i = j
			}
			os.Rename(ofile.Name(), oname)
			ofile.Close()

			for i := 0; i < reply.MapTaskNum; i++ {
				iname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskId)
				err := os.Remove(iname)
				if err != nil {
					log.Fatalf("cannot open delete" + iname)
				}
			}

			// send the finish message to master
			finishedArgs := RequestArgs{-1, reply.ReduceTaskId}
			finishedReply := ReplyArgs{}
			call("Coordinator.ReduceTaskDone", &finishedArgs, &finishedReply)
		}


	}
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
