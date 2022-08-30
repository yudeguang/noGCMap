package noGCMap

import (
	"bytes"
	"github.com/cespare/xxhash"
	"sync"
)

var chunkSize = 64 * 1024
var needDeketeKVPairPersent = 50

const bucketsNum = 512

type NoGcMapAny struct {
	lock                             sync.RWMutex
	len                              int                //记录键值对个数
	mapForHashCollisionAndLongKVPair map[string][]byte  //存放有hash冲突的第2次或2次以上出现的key,以及键值对超长的部分,毕竟超长的部分是少数
	buckets                          [bucketsNum]bucket //数据分片存储
}
type bucket struct {
	lock          sync.RWMutex
	dataBeginPos  uint            //下一个键值对存储的偏移位置
	curChunkIndex int             //最新增加的数据在chunks中的下标
	deleteItemNum int             //删除的键值对个数
	index         map[uint64]uint //索引 值为切片data []byte中的某个位置,此索引存储无hash冲突的key的hash值以及有hash冲突但是是第1次出现的key的hash值
	chunks        [][]byte        //键值对真实的存储位置
}
type Config struct {
	//用于定义当删除的键值对占总键值对的百分比达到多少时，开始一轮真正的删除老旧键值对，并回收内存空间操作
	//该作介于1到100之间
	//默认为50
	NeedDeketeKVPairPersent int
	//用于定义chunkSize
	//最终的chunkSize为 MultiplesOf64KForchunkSize * 64 * 1024
	//该值需要介于1到1000之间(1==>64K,1000==>64M)
	//默认为1，即chunkSize=1 * 64 * 1024
	MultiplesOf64KForchunkSize int
}

//初始化 config配置可选
func New(config ...Config) *NoGcMapAny {
	if len(config) > 0 {
		if config[0].NeedDeketeKVPairPersent > 0 && config[0].NeedDeketeKVPairPersent <= 100 {
			needDeketeKVPairPersent = config[0].NeedDeketeKVPairPersent
		}
		if config[0].MultiplesOf64KForchunkSize > 0 && config[0].MultiplesOf64KForchunkSize <= 1000 {
			chunkSize = config[0].MultiplesOf64KForchunkSize * 64 * 1024
		}
	}
	var n NoGcMapAny
	for i := range n.buckets {
		n.buckets[i].index = make(map[uint64]uint)
	}
	n.mapForHashCollisionAndLongKVPair = make(map[string][]byte)
	return &n
}

//取出数据
func (n *NoGcMapAny) Get(k []byte) (v []byte, exist bool) {
	h := xxhash.Sum64(k)
	idx := h % bucketsNum
	n.buckets[idx].lock.RLock()
	defer n.buckets[idx].lock.RUnlock()
	dataBeginPos, exist := n.buckets[idx].index[h]
	if exist {
		v, exist = n.buckets[idx].read(k, dataBeginPos)
		if exist {
			return
		}
	}
	//分片数据中没找到，下面再从可能存在hash冲突的小表查找
	n.lock.RLock()
	defer n.lock.RUnlock()
	val, exist := n.mapForHashCollisionAndLongKVPair[string(k)]
	if exist {
		return val, exist
	}
	return v, false
}

//取出数据,以string的方式
func (n *NoGcMapAny) GetString(k string) (v string, exist bool) {
	vbyte, exist := n.Get([]byte(k))
	if exist {
		return string(vbyte), true
	}
	return v, false
}

//增加数据
func (n *NoGcMapAny) Set(k, v []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsNum
	kvPairLen := len(k) + len(v) + 5
	//不支持KV长度过大的数据，如果过长，会被存储到mapForHashCollisionAndLongKVPair
	if kvPairLen > chunkSize {
		n.lock.Lock()
		n.mapForHashCollisionAndLongKVPair[string(k)] = v
		n.lock.Unlock()
		return
	}
	n.buckets[idx].lock.Lock()
	defer n.buckets[idx].lock.Unlock()
	dataBeginPos, exist := n.buckets[idx].index[h]
	if exist {
		vv, exist := n.buckets[idx].read(k, dataBeginPos)
		if exist {
			if bytes.Equal(vv, v) {
				//说明是无效的更新
				return
			} else {
				//更新数据，val发生变化，那么先删除，然后在末尾添加一条新数据，并更新索引
				n.buckets[idx].tryDelete(h, k, dataBeginPos, false)
				n.buckets[idx].write(h, k, v)
				return
			}
		} else {
			//这种情况就是存在hash冲突,存储到mapForHashCollisionAndLongKVPair
			n.lock.Lock()
			n.mapForHashCollisionAndLongKVPair[string(k)] = v
			n.lock.Unlock()
			return
		}
	}
	n.buckets[idx].write(h, k, v)
}

//增加数据 以string的形式
func (n *NoGcMapAny) SetString(k, v string) {
	n.Set([]byte(k), []byte(v))
}

//删除数据 以string的形式
func (n *NoGcMapAny) DeleteString(k string) {
	n.Delete([]byte(k))
}

//删除数据
func (n *NoGcMapAny) Delete(k []byte) {
	h := xxhash.Sum64(k)
	idx := h % bucketsNum
	var findInbuckets = false
	n.buckets[idx].lock.Lock()
	defer n.buckets[idx].lock.Unlock()
	dataBeginPos, exist := n.buckets[idx].index[h]
	if exist {
		exist = n.buckets[idx].tryDelete(h, k, dataBeginPos, true)
		if exist {
			findInbuckets = true
		}
	}
	//开始尝试在mapForHashCollisionAndLongKVPair中删除
	if !findInbuckets {
		n.lock.Lock()
		delete(n.mapForHashCollisionAndLongKVPair, string(k))
		n.lock.Unlock()
	}
	//开始清除已删除的键值对,暂时做成需要一次性全部完成，后期在考虑是否做成一次只删除多少条的情况
	if n.buckets[idx].deleteItemNum > 100 && float32(n.buckets[idx].deleteItemNum)/float32(len(n.buckets[idx].index))*100 > float32(needDeketeKVPairPersent) {
		itemNum := 0
		itemNumOfDeleted := 0
		//重置数据
		n.buckets[idx].dataBeginPos = 0
		n.buckets[idx].curChunkIndex = 0
		n.buckets[idx].deleteItemNum = 0
		chunkBuffer := make([]byte, 0, chunkSize)
		KVPairBuffer := make([]byte, 0, chunkSize)
		//处理内存回收
		var hh uint64
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
				KVPairBuffer, hh, hasDeleted = n.readKVPairForGC(chunkBuffer, KVPairBuffer, dataBeginPos)
				if !hasDeleted {
					itemNum++
					n.buckets[idx].writeKVPairForGC(hh, KVPairBuffer)
				} else {
					itemNumOfDeleted++
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
func (b *bucket) tryDelete(h uint64, k []byte, dataBeginPos uint, isFromDelete bool) (exist bool) {
	chunkIndex := int(dataBeginPos) / chunkSize
	dataBeginPos = dataBeginPos % uint(chunkSize)
	//读取键值的长度
	kvLenBuf := b.chunks[chunkIndex][dataBeginPos+1 : dataBeginPos+5] //要去掉第一个用于记录是否被删除的字节
	keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
	//读取键的内容，并判断键是否相同
	dataBeginPos = dataBeginPos + 5
	if bytes.Equal(k, b.chunks[chunkIndex][int(dataBeginPos):int(dataBeginPos)+int(keyLen)]) {
		b.chunks[chunkIndex][dataBeginPos-5] = 1 //值改为1，表示此键值对已经被删除
		b.deleteItemNum++                        //增加计数
		if isFromDelete {
			delete(b.index, h) //删除键 只有在是从Delete这个渠道过来的时候，才删除，否则可能会产生并发读写冲突
		}
		return true
	}
	return false
}

//读取数据
func (b *bucket) read(k []byte, dataBeginPos uint) (v []byte, exist bool) {
	chunkIndex := int(dataBeginPos) / chunkSize
	dataBeginPos = dataBeginPos % uint(chunkSize)
	//读取键值的长度
	kvLenBuf := b.chunks[chunkIndex][dataBeginPos+1 : dataBeginPos+5] //要去掉第一个用于记录是否被删除的字节
	keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
	valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
	//读取键的内容，并判断键是否相同
	dataBeginPos = dataBeginPos + 5
	if !bytes.Equal(k, b.chunks[chunkIndex][int(dataBeginPos):int(dataBeginPos)+int(keyLen)]) {
		return v, false
	}
	//读取值并返回
	if valLen == 0 {
		return nil, true
	}
	dataBeginPos = dataBeginPos + uint(keyLen)
	v = make([]byte, 0, int(valLen))
	v = append(v, b.chunks[chunkIndex][dataBeginPos:dataBeginPos+uint(valLen)]...)
	return v, true
}

//读取数据 用于回收内存空间时
func (n *NoGcMapAny) readKVPairForGC(chunkBuffer, KVPairBuffer []byte, dataBeginPos uint) ([]byte, uint64, bool) {
	hasDeleted := false
	//读取键值的长度 写得能懂直接从fastcache复制过来
	if chunkBuffer[dataBeginPos] == 1 {
		hasDeleted = true
	}
	KVPairBuffer = append(KVPairBuffer, chunkBuffer[dataBeginPos]) //1 isDelete
	kvLenBuf := chunkBuffer[dataBeginPos+1 : dataBeginPos+5]       //要去掉第一个用于记录是否被删除的字节
	keyLen := (uint64(kvLenBuf[0]) << 8) | uint64(kvLenBuf[1])
	valLen := (uint64(kvLenBuf[2]) << 8) | uint64(kvLenBuf[3])
	//读取键的内容，并判断键是否相同
	KVPairBuffer = append(KVPairBuffer, kvLenBuf...) //4 kvLenBuf
	dataBeginPos = dataBeginPos + 5
	h := xxhash.Sum64(chunkBuffer[int(dataBeginPos) : int(dataBeginPos)+int(keyLen)])
	KVPairBuffer = append(KVPairBuffer, chunkBuffer[dataBeginPos:dataBeginPos+uint(keyLen+valLen)]...)
	return KVPairBuffer, h, hasDeleted
}

//写入数据 用于回收内存空间时
func (b *bucket) write(h uint64, k, v []byte) {
	kvPairLen := 5 + len(k) + len(v) //1(该键值对是否已被删除);2(k的长度);2(v的长度);1+2+2=5
	var kvLenBuf [4]byte
	kvLenBuf[0] = byte(uint16(len(k)) >> 8)
	kvLenBuf[1] = byte(len(k))
	kvLenBuf[2] = byte(uint16(len(v)) >> 8)
	kvLenBuf[3] = byte(len(v))
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
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], k...)
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], v...)
	b.index[h] = b.dataBeginPos                       //先存储当前的键值对
	b.dataBeginPos = b.dataBeginPos + uint(kvPairLen) //再更新下一个键值对的偏移
}

//写入数据 用于回收内存空间时
func (b *bucket) writeKVPairForGC(h uint64, KVPairBuffer []byte) {
	kvPairLen := len(KVPairBuffer)
	//当前的chunk是否能存储下相应的键值对,如果存储不下，那么增加一个chunk并且修改curChunkIndex和dataBeginPos
	//这里，空间肯定是足够的，所以不用判断是否需要扩展空间等问题
	if cap(b.chunks[b.curChunkIndex])-len(b.chunks[b.curChunkIndex]) < kvPairLen {
		b.curChunkIndex++
		b.dataBeginPos = uint(chunkSize * (b.curChunkIndex)) //当前键值对将从新的chunk的0位置开始存储数据
	}
	//将原有的数据写入
	b.chunks[b.curChunkIndex] = append(b.chunks[b.curChunkIndex], KVPairBuffer...)
	b.index[h] = b.dataBeginPos                       //先存储当前的键值对
	b.dataBeginPos = b.dataBeginPos + uint(kvPairLen) //再更新下一个键值对的偏移
}
