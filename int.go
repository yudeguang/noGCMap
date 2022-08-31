// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type NoGcMapInt struct {
	lock             sync.RWMutex
	mapForLongKVPair map[int][]byte        //存放有hash冲突的第2次或2次以上出现的key,以及键值对超长的部分,毕竟超长的部分是少数
	buckets          [bucketsNum]bucketInt //数据分片存储
}
type bucketInt struct {
	lock          sync.RWMutex
	dataBeginPos  uint         //下一个键值对存储的偏移位置
	curChunkIndex int          //最新增加的数据在chunks中的下标
	deleteItemNum int          //删除的键值对个数
	index         map[int]uint //索引 值为切片data []byte中的某个位置,此索引存储无hash冲突的key的hash值以及有hash冲突但是是第1次出现的key的hash值
	chunks        [][]byte     //键值对真实的存储位置
}

//初始化 config配置可选
func NewInt(config ...Config) *NoGcMapInt {
	if len(config) > 0 {
		if config[0].NeedDeketeKVPairPersent > 0 && config[0].NeedDeketeKVPairPersent <= 100 {
			needDeketeKVPairPersent = config[0].NeedDeketeKVPairPersent
		}
		if config[0].MultiplesOf64KForchunkSize > 0 && config[0].MultiplesOf64KForchunkSize <= 1000 {
			chunkSize = config[0].MultiplesOf64KForchunkSize * 64 * 1024
		}
	}
	var n NoGcMapInt
	for i := range n.buckets {
		n.buckets[i].index = make(map[int]uint)
	}
	n.mapForLongKVPair = make(map[int][]byte)
	return &n
}

//取出数据
func (n *NoGcMapInt) Get(k int) (v []byte, exist bool) {
	idx := abs(k % bucketsNum)
	n.buckets[idx].lock.RLock()
	defer n.buckets[idx].lock.RUnlock()
	dataBeginPos, exist := n.buckets[idx].index[k]
	if exist {
		v, exist = n.buckets[idx].read(k, dataBeginPos)
		if exist {
			return
		}
	}
	//分片数据中没找到，下面再从可能存在hash冲突的小表查找
	n.lock.RLock()
	defer n.lock.RUnlock()
	val, exist := n.mapForLongKVPair[k]
	if exist {
		return val, exist
	}
	return v, false
}

//取出数据,以string的方式
func (n *NoGcMapInt) GetString(k int) (v string, exist bool) {
	vbyte, exist := n.Get(k)
	if exist {
		return string(vbyte), true
	}
	return v, false
}

//增加数据
func (n *NoGcMapInt) Set(k int, v []byte) {
	idx := abs(k % bucketsNum)
	kvPairLen := len(v) + 11
	//不支持KV长度过大的数据，如果过长，会被存储到mapForHashCollisionAndLongKVPair
	if kvPairLen > chunkSize {
		n.lock.Lock()
		n.mapForLongKVPair[k] = v
		n.lock.Unlock()
		return
	}
	n.buckets[idx].lock.Lock()
	defer n.buckets[idx].lock.Unlock()
	dataBeginPos, exist := n.buckets[idx].index[k]
	if exist {
		vv, exist := n.buckets[idx].read(k, dataBeginPos)
		if exist {
			if bytes.Equal(vv, v) {
				//说明是无效的更新
				return
			} else {
				//更新数据，val发生变化，那么先删除，然后在末尾添加一条新数据，并更新索引
				n.buckets[idx].markAsDelete(k, dataBeginPos, false)
				n.buckets[idx].write(k, v)
				return
			}
		} else {
			//这种情况就是存在hash冲突,存储到mapForHashCollisionAndLongKVPair
			n.lock.Lock()
			n.mapForLongKVPair[k] = v
			n.lock.Unlock()
			return
		}
	}
	n.buckets[idx].write(k, v)
}

//增加数据 以string的形式
func (n *NoGcMapInt) SetString(k int, v string) {
	n.Set(k, []byte(v))
}

//删除数据
func (n *NoGcMapInt) Delete(k int) {
	idx := abs(k % bucketsNum)
	var findInbuckets = false
	n.buckets[idx].lock.Lock()
	defer n.buckets[idx].lock.Unlock()
	dataBeginPos, exist := n.buckets[idx].index[k]
	if exist {
		exist = n.buckets[idx].markAsDelete(k, dataBeginPos, true)
		if exist {
			findInbuckets = true
		}
	}
	//开始尝试在mapForHashCollisionAndLongKVPair中删除
	if !findInbuckets {
		n.lock.Lock()
		delete(n.mapForLongKVPair, k)
		n.lock.Unlock()
	}
	//开始清除已删除的键值对,暂时做成需要一次性全部完成，后期在考虑是否做成一次只删除多少条的情况
	if n.buckets[idx].deleteItemNum > 100 && float32(n.buckets[idx].deleteItemNum)/float32(len(n.buckets[idx].index))*100 > float32(needDeketeKVPairPersent) {
		//重置数据
		n.buckets[idx].dataBeginPos = 0
		n.buckets[idx].curChunkIndex = 0
		n.buckets[idx].deleteItemNum = 0
		chunkBuffer := make([]byte, 0, chunkSize)
		KVPairBuffer := make([]byte, 0, chunkSize)
		//处理内存回收
		var kk int
		var dataBeginPos uint
		var hasDeleted bool
		for i := 0; i < len(n.buckets[idx].chunks); i++ {
			if n.buckets[idx].chunks[i] == nil {
				continue
			}
			dataBeginPos = 0                                               //每一次，新的chunk中，都是从0开始读取的
			chunkBuffer = chunkBuffer[0:0]                                 //清空
			chunkBuffer = append(chunkBuffer, n.buckets[idx].chunks[i]...) //先复制
			n.buckets[idx].chunks[i] = n.buckets[idx].chunks[i][0:0]       //再清空,但先不回收空间,最后如果空间没使用再回收
			for {
				if dataBeginPos >= uint(len(chunkBuffer)) {
					break
				}
				KVPairBuffer = KVPairBuffer[0:0]
				KVPairBuffer, kk, hasDeleted = n.buckets[idx].readKVPairForGC(chunkBuffer, KVPairBuffer, dataBeginPos)
				if !hasDeleted {
					n.buckets[idx].writeKVPairForGC(kk, KVPairBuffer)
				}
				dataBeginPos = dataBeginPos + uint(len(KVPairBuffer))
			}
		}
		//正式回收闲置空间
		for i := 0; i < len(n.buckets[idx].chunks); i++ {
			if len(n.buckets[idx].chunks[i]) == 0 {
				n.buckets[idx].chunks[i] = nil
			}
		}
	}
}

//如果存在某个键:1)相应键值对第一个字节改为1;2)并从索引中删除(只针对从Delete方法);3)增加删除计数器;
func (b *bucketInt) markAsDelete(k int, dataBeginPos uint, isFromDelete bool) (exist bool) {
	chunkIndex := int(dataBeginPos) / chunkSize
	dataBeginPos = dataBeginPos % uint(chunkSize)
	//读取键的内容，并判断键是否相同
	dataBeginPos = dataBeginPos + 3
	if k == bytesToInt(b.chunks[chunkIndex][int(dataBeginPos):int(dataBeginPos)+8]) {
		b.chunks[chunkIndex][dataBeginPos-3] = 1 //值改为1，表示此键值对已经被删除
		b.deleteItemNum++                        //增加计数
		if isFromDelete {
			delete(b.index, k) //删除键 只有在是从Delete这个渠道过来的时候，才删除，否则可能会产生并发读写冲突
		}
		return true
	}
	return false
}

//读取数据
func (b *bucketInt) read(k int, dataBeginPos uint) (v []byte, exist bool) {
	chunkIndex := int(dataBeginPos) / chunkSize
	dataBeginPos = dataBeginPos % uint(chunkSize)
	//读取键值的长度
	kvLenBuf := b.chunks[chunkIndex][dataBeginPos+1 : dataBeginPos+3] //要去掉第一个用于记录是否被删除的字节
	valLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
	//读取键的内容，并判断键是否相同
	dataBeginPos = dataBeginPos + 3
	if k != bytesToInt(b.chunks[chunkIndex][int(dataBeginPos):int(dataBeginPos)+8]) {
		return v, false
	}
	//读取值并返回
	if valLen == 0 {
		return nil, true
	}
	dataBeginPos = dataBeginPos + 8
	v = make([]byte, 0, int(valLen))
	v = append(v, b.chunks[chunkIndex][dataBeginPos:dataBeginPos+uint(valLen)]...)
	return v, true
}

//读取数据 用于回收内存空间时
func (n *bucketInt) readKVPairForGC(chunkBuffer, KVPairBuffer []byte, dataBeginPos uint) ([]byte, int, bool) {
	hasDeleted := false
	//读取键值的长度 写得能懂直接从fastcache复制过来
	if chunkBuffer[dataBeginPos] == 1 {
		hasDeleted = true
	}
	KVPairBuffer = append(KVPairBuffer, chunkBuffer[dataBeginPos]) //1 isDelete
	kvLenBuf := chunkBuffer[dataBeginPos+1 : dataBeginPos+3]       //要去掉第一个用于记录是否被删除的字节
	valLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
	//读取键的内容，并判断键是否相同
	KVPairBuffer = append(KVPairBuffer, kvLenBuf...) //4 kvLenBuf
	dataBeginPos = dataBeginPos + 3
	k := bytesToInt(chunkBuffer[int(dataBeginPos) : int(dataBeginPos)+8])
	KVPairBuffer = append(KVPairBuffer, chunkBuffer[dataBeginPos:dataBeginPos+uint(8+valLen)]...)
	return KVPairBuffer, k, hasDeleted
}

//写入数据
func (b *bucketInt) write(k int, v []byte) {
	kvPairLen := 11 + len(v) //1(该键值对是否已被删除);2(v的长度);8(int的长度)
	var kvLenBuf [2]byte
	kvLenBuf[0] = byte(uint16(len(v)) >> 8)
	kvLenBuf[1] = byte(len(v))
	//第一次的时候，可能还未分配内存
	if b.curChunkIndex == 0 && b.chunks == nil {
		chunk := make([]byte, 0, chunkSize)
		b.chunks = append(b.chunks, chunk)
	}
	//当前的chunk是否能存储下相应的键值对,如果存储不下，那么增加一个chunk并且修改curChunkIndex和dataBeginPos
	if cap(b.chunks[b.curChunkIndex])-len(b.chunks[b.curChunkIndex]) < kvPairLen {
		b.curChunkIndex++
		b.dataBeginPos = uint(chunkSize * (b.curChunkIndex)) //当前键值对将从新的chunk的0位置开始存储数据
		chunk := make([]byte, 0, chunkSize)
		b.chunks = append(b.chunks, chunk)
	}
	//第一个字节表示该键字对是否被删除
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], 0)
	//将数据 append 到 chunk 中
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], kvLenBuf[:]...)
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], intToBytes(k)...)
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], v...)
	b.index[k] = b.dataBeginPos                       //先存储当前的键值对
	b.dataBeginPos = b.dataBeginPos + uint(kvPairLen) //再更新下一个键值对的偏移
}

//写入数据 用于回收内存空间时
func (b *bucketInt) writeKVPairForGC(k int, KVPairBuffer []byte) {
	kvPairLen := len(KVPairBuffer)
	//当前的chunk是否能存储下相应的键值对,如果存储不下，那么增加一个chunk并且修改curChunkIndex和dataBeginPos
	//这里，空间肯定是足够的，所以不用判断是否需要扩展空间等问题
	if cap(b.chunks[b.curChunkIndex])-len(b.chunks[b.curChunkIndex]) < kvPairLen {
		b.curChunkIndex++
		b.dataBeginPos = uint(chunkSize * (b.curChunkIndex)) //当前键值对将从新的chunk的0位置开始存储数据
	}
	//将原有的数据写入
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], KVPairBuffer...)
	b.index[k] = b.dataBeginPos                       //先存储当前的键值对
	b.dataBeginPos = b.dataBeginPos + uint(kvPairLen) //再更新下一个键值对的偏移
}

//返回长度，但不一定是绝对精确
func (n *NoGcMapInt) Len() int {
	n.lock.Lock()
	num := len(n.mapForLongKVPair)
	n.lock.Unlock()
	for i := range n.buckets {
		n.buckets[i].lock.Lock()
		num = num + len(n.buckets[i].index)
		n.buckets[i].lock.Unlock()
	}
	return num
}
func intToBytes(n int) []byte {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	binary.Write(bytebuf, binary.BigEndian, data)
	return bytebuf.Bytes()
}

func bytesToInt(b []byte) int {
	bytebuff := bytes.NewBuffer(b)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return int(data)
}

func abs(i int) int {
	if i < 0 {
		return -i
	}
	return i
}
