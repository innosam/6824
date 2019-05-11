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
    m := make(map[string]int)

    wg.Add(ntasks)
    freeWorkers := make(chan string)
    completed := make(chan struct{})

    var done bool
    done = false

    go func() {
        fmt.Printf("wait group started\n")
        wg.Wait()
        done = true
        close(completed)
        fmt.Printf("wait group done\n")
    }()

    var counter int
    counter = 0

    for {
        if done {
            break
        }

        fmt.Printf("looping %v \n", done)
        select
            {
                case worker := <-registerChan:
                    fmt.Printf("Scheduling on worker: %d \n",worker)
                    // Take a lock before reading the dictionary.
                    if _, ok := m[worker]; ok {
                        break;
                    }
                    m[worker] = 1

                    taskId := counter
                    counter += 1
                    if taskId >=  ntasks{
                        fmt.Printf("All task done\n")
                        break
                    }

                    go func() {
                        fmt.Printf("REGISTERCHAN\n")
                        fmt.Printf("Scheduling %d worker: %d \n", taskId, worker)
                        task := DoTaskArgs{jobName, mapFiles[taskId], phase, taskId, n_other}
                        call(worker,"Worker.DoTask", task, nil)
                        fmt.Printf("TaskComplete %d\n", taskId)
                        wg.Done()
                        freeWorkers<-worker
                        delete(m, worker)
                    }()

                case worker := <-freeWorkers:
                    fmt.Printf("Scheduling on worker: %d \n",worker)
                    // Take a lock before reading the dictionary.
                    if _, ok := m[worker]; ok {
                        break;
                    }

                    taskId := counter
                    counter += 1
                    if taskId >=  ntasks{
                        fmt.Printf("All task done\n")
                        break
                    }

                    go func() {
                        fmt.Printf("FREEWORKERS\n")
                        if _, ok := m[worker]; !ok {
                        fmt.Printf("Scheduling %d worker: %d for %v\n", taskId, worker, phase)
                        m[worker] = 1
                        task := DoTaskArgs{jobName, mapFiles[taskId], phase, taskId, n_other}
                        call(worker,"Worker.DoTask", task, nil)
                        fmt.Printf("TaskComplete %d\n", taskId)
                        wg.Done()
                        freeWorkers<-worker
                        delete(m, worker)
                        }
                    }()
                case <-completed:
                    fmt.Println("completed case.\n")
                    done = true
            }
    }

	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)
}
