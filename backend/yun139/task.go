package yun139

import (
	"context"
	"fmt"
	"time"
)

// taskPollResult mirrors the /hcy/task/get response envelope.
type taskPollResult struct {
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Data    struct {
		TaskInfo struct {
			TaskID string `json:"taskId"`
			Status string `json:"status"` // Running | Succeed | Failed
		} `json:"taskInfo"`
	} `json:"data"`
}

// taskGet polls /hcy/task/get until the asynchronous operation finishes.
//
// Delete / move / copy on 139 are task-based: the mutating call returns
// a taskId immediately and the change happens in the background. rclone
// callers expect the operation to be done when the call returns, so we
// poll. The official client polls ~1/s; we pace ourselves with the
// pacer and give up after 60s.
func (f *Fs) taskGet(ctx context.Context, taskID, what string) error {
	body := map[string]any{"taskId": taskID}
	deadline := time.Now().Add(60 * time.Second)
	for {
		var out taskPollResult
		err := f.pacer.Call(func() (bool, error) {
			err := f.personalCall(ctx, "/hcy/task/get", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return fmt.Errorf("yun139: poll %s task: %w", what, err)
		}
		if !out.Success {
			return &apiError{Code: out.Code, Message: out.Message}
		}
		switch out.Data.TaskInfo.Status {
		case "Succeed", "SUCCESS", "":
			return nil
		case "Failed", "Failure", "failed":
			return fmt.Errorf("yun139: %s task %s failed", what, taskID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yun139: %s task %s did not finish in 60s", what, taskID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}