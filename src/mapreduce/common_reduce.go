package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	var reduceKVs []KeyValue

	// 从每个 map task 生成的文件里读
	for mapTask := 0; mapTask < nMap; mapTask++ {
		imterFilename := reduceName(jobName, mapTask, reduceTask)

		if _, err := os.Stat(imterFilename); os.IsNotExist(err) {
			continue
		}
		var _reduceKVs []KeyValue
		f, err := ioutil.ReadFile(imterFilename)
		if err != nil {
			log.Fatalf("doReduce: fatail when open imter file: %s, %v\n", imterFilename, err)
		}
		err = json.Unmarshal(f, &_reduceKVs)
		if err != nil {
			log.Fatalf("doReduce: fatail when unmarshal imter file: %s, %v\n", imterFilename, err)
		}

		reduceKVs = append(reduceKVs, _reduceKVs...)
	}

	// 把相同 key 合并
	sortedKVs := make(map[string][]string)

	for _, kv := range reduceKVs {
		if _, ok := sortedKVs[kv.Key]; !ok {
			sortedKVs[kv.Key] = []string{kv.Value}
		} else {
			sortedKVs[kv.Key] = append(sortedKVs[kv.Key], kv.Value)
		}
	}

	fo, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	defer fo.Close()
	if err != nil {
		log.Fatalf("doReduce: fatail when open res %s, %v\n", outFile, err)
	}
	enc := json.NewEncoder(fo)

	for key, values := range sortedKVs {
		enc.Encode(KeyValue{Key: key, Value: reduceF(key, values)})
	}
}
