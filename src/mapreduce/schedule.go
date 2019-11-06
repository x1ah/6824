package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	var workers []string
	quit := make(chan string)

	go func() {
		select {
		case w := <-registerChan:
			workers = append(workers, w)
		case <-quit:
			return
		}
	}()

	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		args := &DoTaskArgs{
			JobName:       jobName,
			Phase:         phase,
			TaskNumber:    i,
			NumOtherPhase: n_other,
		}
		if i >= len(mapFiles) {
			args.File = ""
		} else {
			args.File = mapFiles[i]
		}
		go func() {
			defer wg.Done()

			// 拿一个 worker
			workerAddr := <-registerChan
			fmt.Printf("get worker: %s\n", workerAddr)
			ok := call(workerAddr, "Worker.DoTask", args, nil)

			// 跑完回收 worker
			go func() {
				registerChan <- workerAddr
			}()
			if !ok {
				fmt.Printf("Worker.DoTask faild: %s\n", workerAddr)
			}
		}()
	}
	wg.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
