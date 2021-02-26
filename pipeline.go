package sync

import (
	"context"
	"grabview/common/log"
	"runtime"
	"sync"

	"github.com/pkg/errors"
)

var (
	ErrNoStage = errors.New("the pipeline has no stage")
	ErrNoTask  = errors.New("the stage has no task")
)

type StaticTask func(ctx context.Context, input interface{}) (output interface{})
type runChain func(ctx context.Context, inputChan <-chan interface{}) (outputChan <-chan interface{})

type Stage struct {
	Task    StaticTask
	Workers uint
}

func (s *Stage) singleTaskChain() runChain {
	return func(ctx context.Context, inputChan <-chan interface{}) (outputChan <-chan interface{}) {
		resChan := make(chan interface{})

		go func() {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 64<<10)
					buf = buf[:runtime.Stack(buf, false)]
					log.Error(ctx, "pipeline: panic recovered: %s\n%s", r, buf)
				}
				close(resChan)
			}()

			for input := range inputChan {
				if outObj := s.Task(ctx, input); outObj != nil {
					select {
					case resChan <- outObj:
					case <-ctx.Done():
					}

				}
			}
		}()

		return resChan
	}
}

func (s *Stage) groupTasksChain() runChain {
	singleTaskChain := s.singleTaskChain()
	return func(ctx context.Context, inputChan <-chan interface{}) (outputChan <-chan interface{}) {
		var chanList []<-chan interface{}
		for i := uint(0); i < s.Workers; i++ {
			chanList = append(chanList, singleTaskChain(ctx, inputChan))
		}
		outputChan = fanIn(ctx, chanList...)
		return
	}
}

func (s *Stage) runningMode() runChain {
	if s.Workers > 1 {
		return s.groupTasksChain()
	}
	return s.singleTaskChain()
}

type Pipeline struct {
	Stages []*Stage
}

func (p *Pipeline) AddStage(stage *Stage) {
	p.Stages = append(p.Stages, stage)
}

func (p *Pipeline) Run(ctx context.Context, msg <-chan interface{}) (<-chan interface{}, error) {
	if len(p.Stages) == 0 {
		return nil, ErrNoStage
	}

	for _, stage := range p.Stages {
		if stage.Task == nil {
			return nil, ErrNoTask
		}
	}

	for _, stage := range p.Stages {
		msg = stage.runningMode()(ctx, msg)
	}

	return msg, nil
}

// fan-in merge the input channels to one output channel
func fanIn(ctx context.Context, inputChans ...<-chan interface{}) <-chan interface{} {
	var wg sync.WaitGroup
	wg.Add(len(inputChans))

	outputChan := make(chan interface{})

	outputFn := func(input <-chan interface{}) {
		for v := range input {
			select {
			case outputChan <- v:
			case <-ctx.Done():
			}
		}
		wg.Done()
	}

	for _, inputChan := range inputChans {
		go outputFn(inputChan)
	}

	go func() {
		wg.Wait()
		close(outputChan)
	}()

	return outputChan
}
