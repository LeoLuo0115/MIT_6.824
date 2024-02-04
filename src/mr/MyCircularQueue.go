package mr

type MyCircularQueue struct {
	hh int
	tt int
	q  []int
}

func MQConstructor(k int) MyCircularQueue {
	return MyCircularQueue{
		hh: 0,
		tt: 0,
		q:  make([]int, k+1), // 多开一个位置
	}
}

func (mq *MyCircularQueue) EnQueue(value int) bool {
	if mq.IsFull() {
		return false
	}
	mq.q[mq.tt] = value // 入队顺时针走队尾
	mq.tt++              // tt指针向后移动
	if mq.tt == len(mq.q) {
		mq.tt = 0 // 走出最后就回到0
	}
	return true
}

func (mq *MyCircularQueue) DeQueue() bool {
	if mq.IsEmpty() {
		return false
	}
	mq.hh++ // 删除队头
	if mq.hh == len(mq.q) {
		mq.hh = 0
	}
	return true
}

func (mq *MyCircularQueue) Front() int {
	if mq.IsEmpty() {
		return -1
	}
	return mq.q[mq.hh] // 返回队头
}

func (mq *MyCircularQueue) Rear() int {
	if mq.IsEmpty() {
		return -1
	}
	t := mq.tt - 1
	if t < 0 {
		t = len(mq.q) - 1
	}
	return mq.q[t] // 返回队尾
}

func (mq *MyCircularQueue) IsEmpty() bool {
	// 两个指针在同一个位置时为空
	return mq.tt == mq.hh
}

func (mq *MyCircularQueue) IsFull() bool {
	// 队尾指针的下一个位置是队头时为满
	return (mq.tt+1)%len(mq.q) == mq.hh
}

func (mq *MyCircularQueue) Size() int {
	return (mq.tt - mq.hh + len(mq.q)) % len(mq.q)
}