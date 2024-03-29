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

	var wg sync.WaitGroup
	wg.Add(ntasks)
	taskChan := make(chan int, ntasks)
	quit := make(chan int)

	// All tasks
	for i := 0; i < ntasks; i++ {
		taskChan <- i
	}

	go func() {
		for {
			select {
			case taskNumber := <-taskChan:
				args := &DoTaskArgs{
					JobName:       jobName,
					Phase:         phase,
					TaskNumber:    taskNumber,
					NumOtherPhase: n_other,
				}
				if taskNumber >= len(mapFiles) {
					args.File = ""
				} else {
					args.File = mapFiles[taskNumber]
				}
				go func() {
					// 拿一个 worker
					workerAddr := <-registerChan
					ok := call(workerAddr, "Worker.DoTask", args, nil)

					// 跑完回收 worker
					go func() {
						registerChan <- workerAddr
					}()

					if !ok {
						// 失败的重新放进 chan 里
						taskChan <- taskNumber
					} else {
						wg.Done()
						fmt.Printf("task %d completed\n", taskNumber)
					}
				}()
			case <-quit:
				return
			}
		}
	}()

	// All task completed.
	wg.Wait()
	quit <- 1

	fmt.Printf("Schedule: %v done\n", phase)
}
