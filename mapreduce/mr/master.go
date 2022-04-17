package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// MasterTaskStatus master 任务状态
type MasterTaskStatus int

const (
	Idle       MasterTaskStatus = iota // 休眠
	InProgress                         // 运行
	Completed                          // 完成
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Master struct {
	TaskQueue     chan *Task          // 等待执行的任务
	TaskMeta      map[int]*MasterTask // 当前所有task的信息
	MasterPhase   State
	NReduce       int
	InputFiles    []string
	Intermediates [][]string // Map 任务产生的R 个中间文件的信息
}

type MasterTask struct {
	TaskStatus    MasterTaskStatus
	StartTime     time.Time
	TaskReference *Task
}

type Task struct {
	Input         string
	TaskState     State
	NReducer      int
	TaskNumber    int
	Intermediates []string
	Output        string
}

var mu sync.Mutex

// server ...
func (m *Master) server() {
	_ = rpc.Register(m)
	rpc.HandleHTTP()
	sockName := masterSock()
	_ = os.Remove(sockName)
	l, e := net.Listen("unix", sockName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatal("http serve err", err)
		}
	}()
}

func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	return m.MasterPhase == Exit
}

func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskQueue:     make(chan *Task, max(nReduce, len(files))),
		TaskMeta:      make(map[int]*MasterTask),
		MasterPhase:   Map,
		NReduce:       nReduce,
		InputFiles:    files,
		Intermediates: make([][]string, nReduce),
	}

	// 创建Map 任务
	m.createMapTask()

	// 启动master
	m.server()

	go m.catchTimeout()

	return &m
}

func (m *Master) createMapTask() {
	for i, file := range m.InputFiles {
		taskMeta := Task{
			Input:      file,
			TaskState:  Map,
			NReducer:   m.NReduce,
			TaskNumber: i,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[i] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask)
	for i, files := range m.Intermediates {
		taskMeta := Task{
			TaskState:     Reduce,
			NReducer:      m.NReduce,
			TaskNumber:    i,
			Intermediates: files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[i] = &MasterTask{
			TaskStatus:    Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (m *Master) catchTimeout() {
	for {
		time.Sleep(time.Second * 5)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}

		for _, task := range m.TaskMeta {
			if task.TaskStatus == InProgress && time.Now().Sub(task.StartTime) > 10*time.Second {
				m.TaskQueue <- task.TaskReference
				task.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (m *Master) AssignTask(arg *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime = time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		*reply = Task{
			TaskState: Wait,
		}
	}

	return nil
}

func (m *Master) TaskComplete(task *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		// 重复结果丢弃
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil
}

func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()

	switch task.TaskState {
	case Map:
		for i, filePath := range task.Intermediates {
			m.Intermediates[i] = append(m.Intermediates[i], filePath)
		}
		if m.allTaskDone() {
			// 进入reduce 阶段
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			// 进入exit 阶段
			m.MasterPhase = Exit
		}
	}
}

func (m *Master) allTaskDone() bool {
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}
