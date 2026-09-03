package yun139

import (
	"context"
	"errors"
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

// taskStatusDone reports whether a task/get status means the operation
// completed. The official client emits "Succeed"; "" appears when the
// response omits taskInfo.status (synchronous completion).
func taskStatusDone(status string) bool {
	switch status {
	case "Succeed", "SUCCESS", "success", "":
		return true
	}
	return false
}

// taskStatusFailed reports whether a task/get status means the operation
// failed.
func taskStatusFailed(status string) bool {
	switch status {
	case "Failed", "Failure", "failed":
		return true
	}
	return false
}

// taskGet polls /hcy/task/get until the task finishes.
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
		switch {
		case taskStatusDone(out.Data.TaskInfo.Status):
			return nil
		case taskStatusFailed(out.Data.TaskInfo.Status):
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

// familyBatchReq is the request body for createBatchOprTaskV2, the
// family-cloud equivalent of batchMove / batchCopy / batchDelete
// (captured 2026-09-03).
//
//	taskType: 1 = copy, 2 = delete. The server uses the sourceType +
//	  destType pair to cross spaces: sourceType=3,destType=1 copies
//	  from personal into family; both 1 copies within family; both
//	  empty omits a side.
type familyBatchReq struct {
	ContentList       []string
	CatalogList       []string
	DestCloudID       string
	DestCatalogType   int
	DestType          string
	DestPath          string
	SourceCatalogType int
	SourceCloudID     string
	SourceType        string
	Path              string
	TaskType          int
	BusinessType      int
}

// familyBatchOprTask issues POST
// /hcy/family/adapter/andAlbum/openApi/createBatchOprTaskV2 and
// returns the task id.
func (f *Fs) familyBatchOprTask(ctx context.Context, req familyBatchReq) (string, error) {
	if req.ContentList == nil {
		req.ContentList = []string{}
	}
	if req.CatalogList == nil {
		req.CatalogList = []string{}
	}
	body := map[string]any{
		"catalogList":       req.CatalogList,
		"contentList":       req.ContentList,
		"destCatalogType":   req.DestCatalogType,
		"destCloudID":       req.DestCloudID,
		"destPath":          req.DestPath,
		"destType":          req.DestType,
		"path":              req.Path,
		"sourceCatalogType": req.SourceCatalogType,
		"sourceCloudID":     req.SourceCloudID,
		"sourceType":        req.SourceType,
		"taskType":          req.TaskType,
		"commonAccountInfo": map[string]any{
			"userDomainId": f.userDomainID,
			"accountType":  1,
		},
		"businessType": req.BusinessType,
		"userDomainId": f.userDomainID,
	}
	var resp struct {
		Result struct {
			ResultCode string `json:"resultCode"`
			ResultDesc string `json:"resultDesc"`
		} `json:"result"`
		TaskID string `json:"taskID"`
	}
	err := f.pacer.Call(func() (bool, error) {
		err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/createBatchOprTaskV2", body, &resp)
		return shouldRetry(ctx, err)
	})
	if err != nil {
		return "", err
	}
	if resp.Result.ResultCode != "0" {
		return "", &apiError{Code: resp.Result.ResultCode, Message: resp.Result.ResultDesc}
	}
	if resp.TaskID == "" {
		return "", errors.New("yun139: createBatchOprTaskV2 returned no taskID")
	}
	return resp.TaskID, nil
}

// familyTaskPoll polls queryBatchOprTaskDetailV3 until the task
// completes. State machine (captured 2026-09-03):
//
//	taskStatus 0 = running, 1 = running, 2 = success,
//	taskResultCode 1 means success, 0 means failed.
//	contentList[].reason == "0000" is success; any other code is the
//	per-item error.
func (f *Fs) familyTaskPoll(ctx context.Context, taskID string) error {
	deadline := time.Now().Add(60 * time.Second)
	for {
		body := map[string]any{
			"taskID":    taskID,
			"accountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  1,
			},
			"taskId": taskID,
			"commonAccountInfo": map[string]any{
				"userDomainId": f.userDomainID,
				"accountType":  1,
			},
		}
		var out struct {
			Result struct {
				ResultCode string `json:"resultCode"`
				ResultDesc string `json:"resultDesc"`
			} `json:"result"`
			BatchOprTask struct {
				TaskStatus   int `json:"taskStatus"`
				TaskResultCode *int `json:"taskResultCode"`
			} `json:"batchOprTask"`
			ContentList []struct {
				SrcID  string `json:"srcID"`
				RstID  string `json:"rstID"`
				Reason string `json:"reason"`
			} `json:"contentList"`
		}
		err := f.pacer.Call(func() (bool, error) {
			err := f.familyCall(ctx, "/hcy/family/adapter/andAlbum/openApi/queryBatchOprTaskDetailV3", body, &out)
			return shouldRetry(ctx, err)
		})
		if err != nil {
			return fmt.Errorf("yun139: poll family task: %w", err)
		}
		if out.Result.ResultCode != "0" {
			return &apiError{Code: out.Result.ResultCode, Message: out.Result.ResultDesc}
		}
		switch out.BatchOprTask.TaskStatus {
		case 0, 1:
			// 0 = created, 1 = running - keep polling
		case 2:
			// Success only when resultCode==1.
			if out.BatchOprTask.TaskResultCode != nil && *out.BatchOprTask.TaskResultCode == 1 {
				return nil
			}
			return fmt.Errorf("yun139: family task %s finished with resultCode %v", taskID, out.BatchOprTask.TaskResultCode)
		case 3:
			return fmt.Errorf("yun139: family task %s failed", taskID)
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("yun139: family task %s did not finish in 60s", taskID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}