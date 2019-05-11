package mapreduce

import "fmt"
import "sync"
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

    // Each file corresponds to a single map task.
    // Get all the workers.
    // Keep track of all task available for execution in a list.
    // Keep track of all the task curently in execution and its status using a map.
    // Keep track of all completed task in a list.
    // Worker.DoTask RPC, DoTaskArgs
    // In case of map files, use the mapFiles to submit a task.
    // if a worker is available
    //     - execute a task if its available for execution
    //         add a task to the waitgroup.
    //         start the task using the rpc.
    //         move the task to complete group.
    //     - exit if all are completed.
	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

    var wg sync.WaitGroup
    wg.Add(ntasks)

    m := make(map[string]int)
    completed := make(chan struct{})
    done := false

    tasks := make(chan int,ntasks)
    for i:=0;i<ntasks;i++ {
        tasks <- i
    }

    go func() {
        fmt.Printf("wait group started\n")
        wg.Wait()
        done = true
        close(completed)
        fmt.Printf("wait group done\n")
    }()


    for {
        if done {
            fmt.Printf("Exiting the for loop.\n")
            break
        }

        fmt.Printf("looping %v \n", done)
        select
            {
                case worker := <-registerChan:
                    if _, ok := m[worker]; ok {
                        break;
                    }
                    m[worker] = 1

                    var taskId int
                    select {
                        case taskId = <-tasks:
                           fmt.Printf("Task %d available.\n", taskId)

                        default:
                           taskId = -1
                    }

                    if taskId >=  ntasks{
                        fmt.Printf("All task done\n")
                        break
                    }

                    if taskId == -1 {
                        fmt.Printf("No task available.")
                        break
                    }

                    go func() {
                        fmt.Printf("Scheduling %d worker: %d \n", taskId, worker)
                        task := DoTaskArgs{jobName, mapFiles[taskId], phase, taskId, n_other}
                        call(worker,"Worker.DoTask", task, nil)
                        fmt.Printf("TaskComplete %d\n", taskId)
                        wg.Done()
                        delete(m, worker)
                        registerChan<-worker
                    }()
                case <-completed:
                    fmt.Printf("Completed.\n")
            }
    }

	fmt.Printf("Schedule: %v done\n", phase)
}
