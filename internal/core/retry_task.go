package core

func WithRawMember(task RetryTask, member string) RetryTask {
	task.rawMember = member
	return task
}

func RawMember(task RetryTask) string {
	return task.rawMember
}
