package mapreduce

import (
	"log"
	"net"
	"net/rpc"
	"os"
	"time"
)

// RunSequential will ensure that map and reduce function runs in
// a single-core linearly. The Task is passed from the calling package
// and should contains the definitions for all the required functions
// and parameters.
// Notice that this implementation will store data locally. In the distributed
// version of mapreduce it's common to store the data in the same worker that computed
// it and just pass a reference to reduce jobs so they can go grab it.
func RunSequential(task *Task) {
	var (
		mapCounter int = 0
		mapResult  []KeyValue
	)

	log.Print("Running RunSequential...")

	_ = os.Mkdir(REDUCE_PATH, os.ModePerm)

	for v := range task.InputChan {
		mapResult = task.Map(v)
		storeLocal(task, mapCounter, mapResult)
		mapCounter++
	}

	mergeLocal(task, mapCounter)

	for r := 0; r < task.NumReduceJobs; r++ {
		data := loadLocal(r)
		task.OutputChan <- task.Reduce(data)
	}

	close(task.OutputChan)
	return
}

// RunMaster will start a master node on the map reduce operations.
// In the distributed model, a Master should serve multiple workers and distribute
// the operations to be executed in order to complete the task.
// 	- task: the Task object that contains the mapreduce operation.
//  - hostname: the tcp/ip address on which it will listen for connections.
func RunMaster(task *Task, hostname string) {
	var (
		err          error
		master       *Master
		newRpcServer *rpc.Server
		listener     net.Listener
	)

	log.Println("Running Master on", hostname)

	// Create a reduce directory to store intemediate reduce files.
	_ = os.Mkdir(REDUCE_PATH, os.ModePerm)

	master = newMaster(hostname)

	newRpcServer = rpc.NewServer()
	newRpcServer.Register(master)

	if err != nil {
		log.Panicln("Failed to register RPC server. Error:", err)
	}

	master.rpcServer = newRpcServer

	listener, err = net.Listen("tcp", master.address)

	if err != nil {
		log.Panicln("Failed to start TCP server. Error:", err)
	}

	master.listener = listener

	go master.acceptMultipleConnections()

	go master.handleFailingWorkers()

	master.scheduleMaps(task)

	// Merge the result of multiple map operation with the same reduceId into a single file
	mergeLocal(task, master.mapCounter)

	master.scheduleReduces(task)

	log.Println("Closing Remote Workers.")
	for _, worker := range master.workers {
		err = worker.callRemoteWorker("Worker.Done", new(struct{}), new(struct{}))
		if err != nil {
			log.Panicln("Failed to close Remote Worker. Error:", err)
		}
	}

	log.Println("Done.")
	return
}

// RunWorker will run a instance of a worker. It'll initialize and then try to register with
// master.
// Induced failures:
// -> nOps = number of operations to run before failure (0 = no failure)
// -> during = if failure should occur during next task
func RunWorker(task *Task, hostname string, masterHostname string, nOps int, during bool) {
	var (
		err           error
		worker        *Worker
		rpcs          *rpc.Server
		listener      net.Listener
		retryDuration time.Duration
	)

	log.Println("Running Worker on", hostname)

	_ = os.Mkdir(REDUCE_PATH, os.ModePerm)

	worker = new(Worker)
	worker.hostname = hostname
	worker.masterHostname = masterHostname
	worker.task = task
	worker.done = make(chan bool)

	// Should induce a failure
	if nOps > 0 {
		worker.taskCounter = 0
		worker.nOps = nOps
		worker.during = during
	}

	rpcs = rpc.NewServer()
	rpcs.Register(worker)

	worker.rpcServer = rpcs

	listener, err = net.Listen("tcp", worker.hostname)

	if err != nil {
		log.Panic("Starting RPC listener failed. Error:", err)
	}

	worker.listener = listener
	defer worker.listener.Close()

	retryDuration = time.Duration(2) * time.Second
	for {
		err = worker.register()

		if err == nil {
			break
		}

		log.Printf("Registration failed. Retrying in %v seconds...\n", retryDuration)
		time.Sleep(retryDuration)
	}

	go worker.acceptMultipleConnections()

	<-worker.done
}
