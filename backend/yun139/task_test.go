package yun139

import (
	"encoding/json"
	"testing"
)

// TestTaskStatusDone pins the accepted "done" status values to what the
// official client's task/get returns (captured 2026-09-03): "Succeed"
// (capitalized exactly so), and "" for responses that omit taskInfo.
func TestTaskStatusDone(t *testing.T) {
	done := []string{"Succeed", "SUCCESS", "success", ""}
	for _, s := range done {
		if !taskStatusDone(s) {
			t.Errorf("taskStatusDone(%q) = false, want true", s)
		}
	}
	notDone := []string{"Running", "running", "Pending", "Waiting", "Process"}
	for _, s := range notDone {
		if taskStatusDone(s) {
			t.Errorf("taskStatusDone(%q) = true, want false", s)
		}
	}
}

// TestTaskStatusFailed pins the accepted "failed" status values. The
// official client emits "Failed"; the family cloud has been observed
// emitting "Failure".
func TestTaskStatusFailed(t *testing.T) {
	failed := []string{"Failed", "Failure", "failed"}
	for _, s := range failed {
		if !taskStatusFailed(s) {
			t.Errorf("taskStatusFailed(%q) = false, want true", s)
		}
	}
	notFailed := []string{"Succeed", "", "Running"}
	for _, s := range notFailed {
		if taskStatusFailed(s) {
			t.Errorf("taskStatusFailed(%q) = true, want false", s)
		}
	}
}

// TestTaskPollResultJSON pins the task/get response shape from the
// capture: {"success":true,"code":"0000","data":{"taskInfo":{"taskId":...,"status":"Running"}}}.
func TestTaskPollResultJSON(t *testing.T) {
	body := `{"success":true,"code":"0000","message":"成功","data":{"taskInfo":{"taskId":"2031339331821699713","status":"Running"}}}`
	var out taskPollResult
	if err := jsonUnmarshalStrict(t, body, &out); err != nil {
		t.Fatal(err)
	}
	if !out.Success {
		t.Error("Success = false, want true")
	}
	if out.Data.TaskInfo.TaskID != "2031339331821699713" {
		t.Errorf("TaskID = %q, want 2031339331821699713", out.Data.TaskInfo.TaskID)
	}
	if out.Data.TaskInfo.Status != "Running" {
		t.Errorf("Status = %q, want Running", out.Data.TaskInfo.Status)
	}
}

// TestTaskPollResultJSON_Error pins the error envelope: a task/get that
// returns success:false must surface code+message, not hang.
func TestTaskPollResultJSON_Error(t *testing.T) {
	body := `{"success":false,"code":"1010220314","message":"家庭云不存在","data":null}`
	var out taskPollResult
	if err := jsonUnmarshalStrict(t, body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Success {
		t.Error("Success = true, want false")
	}
	if out.Code != "1010220314" {
		t.Errorf("Code = %q, want 1010220314", out.Code)
	}
	if out.Message != "家庭云不存在" {
		t.Errorf("Message = %q, want 家庭云不存在", out.Message)
	}
}

func jsonUnmarshalStrict(t *testing.T, body string, out any) error {
	t.Helper()
	return json.Unmarshal([]byte(body), out)
}
