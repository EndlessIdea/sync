package sync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipelineRun(t *testing.T) {
	sqPl := Pipeline{}

	tenTimesStage := &Stage{
		Task: func(ctx context.Context, input interface{}) (output interface{}) {
			n, ok := input.(int)
			if !ok {
				return 0
			}
			return n * 10
		},
		Workers: 0,
	}

	squareStage := &Stage{
		Task: func(ctx context.Context, input interface{}) (output interface{}) {
			n, ok := input.(int)
			if !ok {
				return 0
			}
			return n * n
		},
		Workers: 2,
	}

	gen := func(nums ...int) <-chan interface{} {
		out := make(chan interface{})
		go func() {
			defer close(out)

			for _, n := range nums {
				out <- n
			}
		}()
		return out
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sqPl.AddStage(tenTimesStage)
	sqPl.AddStage(squareStage)
	result, err := sqPl.Run(ctx, gen(1, 2, 3, 4, 5))
	assert.Nil(t, err)

	first := <-result
	firstNum, ok := first.(int)
	assert.Equal(t, true, ok)
	assert.Contains(t, []int{100, 400, 900, 1600, 2500}, firstNum)
}
