package mapreduce

import (
	"log"
	"path/filepath"
	"sync"
)

// Schedules map operations on remote workers. This will run until InputFilePathChan
func (master *Master) scheduleMaps(task *Task) {
	// is closed. If there is no worker available, it'll block.
	var (
		wg        sync.WaitGroup
		filePath  string
		worker    *RemoteWorker
		operation *MapOperation
	)

	log.Println("Running map operations")

	for filePath = range task.InputFilePathChan {
		operation = &MapOperation{master.mapCounter, filePath}
		master.mapCounter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runMap(worker, operation, &wg)
	}

	go func() {
		for operation := range master.retryMapOperation {
			worker = <-master.idleWorkerChan
			log.Printf("Retrying map operation %v\n", operation.id)
			go master.runMap(worker, operation, &wg)
		}
	}()

	wg.Wait()
	close(master.retryMapOperation)

	log.Println("Map Completed")
}

// runMap start a single map operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runMap(remoteWorker *RemoteWorker, operation *MapOperation, wg *sync.WaitGroup) {
	var (
		err  error
		args *RunMapArgs
	)

	log.Printf("Running Map '%v' file '%v' on worker '%v'\n", operation.id, operation.filePath, remoteWorker.id)

	args = &RunMapArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker("Worker.RunMap", args, new(struct{}))

	if err != nil {
		log.Printf("Map %v Failed. Error: %v\n", operation.id, err)
		master.retryMapOperation <- operation
		master.failedWorkerChan <- remoteWorker
	} else {
		wg.Done()
		master.idleWorkerChan <- remoteWorker
	}
}

// Schedules reduce operations on remote workers. This will run until reduceFilePathChan
// is closed. If there is no worker available, it'll block.
func (master *Master) scheduleReduces(task *Task) {
	var (
		wg                 sync.WaitGroup
		filePath           string
		worker             *RemoteWorker
		operation          *ReduceOperation
		reduceFilePathChan chan string
	)

	log.Println("Running reduce operations")

	reduceFilePathChan = fanReduceFilePath(task.NumReduceJobs)

	for filePath = range reduceFilePathChan {
		operation = &ReduceOperation{master.reduceCounter, filePath}
		master.reduceCounter++

		worker = <-master.idleWorkerChan
		wg.Add(1)
		go master.runReduce(worker, operation, &wg)
	}

	wg.Wait()
	log.Println("Reduce Completed")
}

// runMap start a single map operation on a RemoteWorker and wait for it to return or fail.
func (master *Master) runReduce(remoteWorker *RemoteWorker, operation *ReduceOperation, wg *sync.WaitGroup) {
	var (
		err  error
		args *RunReduceArgs
	)

	log.Printf("Running Reduce '%v' file '%v' on worker '%v'\n", operation.id, operation.filePath, remoteWorker.id)

	args = &RunReduceArgs{operation.id, operation.filePath}
	err = remoteWorker.callRemoteWorker("Worker.RunReduce", args, new(struct{}))

	if err != nil {
		log.Println("Worker.RunReduce Failed. Error:", err)
	}

	wg.Done()
	master.idleWorkerChan <- remoteWorker
}

// FanIn is a pattern that will return a channel in which the goroutines generated here will keep
// writing until the loop is done.
// This is used to generate the name of all the reduce files.
func fanReduceFilePath(numReduceJobs int) chan string {
	var (
		outputChan chan string
		filePath   string
	)

	outputChan = make(chan string)

	go func() {
		for i := 0; i < numReduceJobs; i++ {
			filePath = filepath.Join(REDUCE_PATH, mergeReduceName(i))

			outputChan <- filePath
		}

		close(outputChan)
	}()
	return outputChan
}
