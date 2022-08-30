package noGCMap

import (
	"log"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile | log.Ltime)
}
func TestAny(t *testing.T) {
	//构建值
	var str string
	var bigStr string
	for i := 0; i <= 10; i++ {
		str = str + "x"
	}
	str = ":" + str + ":"
	for i := 0; i <= 100000; i++ {
		bigStr = bigStr + "A"
	}
	//定义并发数及测试量
	chanNum := 10
	var max = 1000000
	var maxDeleted = 500000
	var mapOffical = make(map[string]string)
	var mapAny = New()
	log.Println("开始测试并发写")
	//set
	var wgSet sync.WaitGroup
	wgSet.Add(chanNum)
	mapAny.SetString("", "empty")
	mapAny.SetString("empty", "")
	mapAny.SetString("bigStr", bigStr)

	//offical
	for i := 0; i < max; i++ {
		mapOffical[strconv.Itoa(i)] = "@" + strconv.Itoa(i) + str
	}
	//mapAny
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := 0; i < max; i++ {
				mapAny.SetString(strconv.Itoa(i), "@"+strconv.Itoa(i)+str)
			}
			wgSet.Done()
		}(i, &wgSet)
	}
	wgSet.Wait()
	log.Println("开始测试并发读取")
	//get
	var wgGet sync.WaitGroup
	wgGet.Add(chanNum)
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := 0; i < max+100; i++ {
				valOffical, existOffical := mapOffical[strconv.Itoa(i)]
				valAny, existAny := mapAny.GetString(strconv.Itoa(i))
				if existOffical != existAny {
					t.Fatalf("unexpected value obtained; got %v want %v", existAny, existOffical)
				}
				if valOffical != valAny {
					t.Fatalf("unexpected value obtained; got %q want %q", valAny, valOffical)
				}
			}
			wgGet.Done()
		}(i, &wgGet)
	}
	wgGet.Wait()
	//get value empty
	valEmpty, exist := mapAny.GetString("empty")
	if !exist {
		t.Fatalf("unexpected value obtained; got %v want %v", exist, true)
	}
	if valEmpty != "" {
		t.Fatalf("unexpected value obtained; got %q want %q", valEmpty, "")
	}

	//get key empty
	valOFKeyEmpty, exist := mapAny.GetString("")
	if !exist {
		t.Fatalf("unexpected value obtained; got %v want %v", exist, true)
	}
	if valOFKeyEmpty != "empty" {
		t.Fatalf("unexpected value obtained; got %q want %q", valOFKeyEmpty, "empty")
	}
	//算一下key个数
	var keyNum, deleteItemNum int
	for i := range mapAny.buckets {
		keyNum = keyNum + len(mapAny.buckets[i].index)
		deleteItemNum = deleteItemNum + mapAny.buckets[i].deleteItemNum
	}
	keyNum = keyNum + len(mapAny.mapForHashCollisionAndLongKVPair)

	if keyNum != max+3 {
		t.Fatalf("unexpected value obtained; got %v want %v", keyNum, max+3)
	}
	log.Println("开始测试并发删除")
	//delete
	var wgDelete sync.WaitGroup
	wgDelete.Add(chanNum)
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := -100; i < maxDeleted; i++ {
				mapAny.DeleteString(strconv.Itoa(i))
				_, exist := mapAny.GetString(strconv.Itoa(i))
				if exist {
					t.Fatalf("unexpected value obtained; got %v want %v", exist, false)
				}
			}
			wgDelete.Done()
		}(i, &wgDelete)
	}
	wgDelete.Wait()
	for i := -100; i < maxDeleted; i++ {
		delete(mapOffical, strconv.Itoa(i))
	}
	log.Println("开始测试并发删除之后与官方结果对比")
	//get 删除之后再用get测试
	var wgGetAfterDeleted sync.WaitGroup
	wgGetAfterDeleted.Add(chanNum)
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := 0; i < max+100; i++ {
				valOffical, existOffical := mapOffical[strconv.Itoa(i)]
				valAny, existAny := mapAny.GetString(strconv.Itoa(i))
				if existOffical != existAny {
					t.Fatalf("unexpected value obtained; got %v want %v", existAny, existOffical)
				}
				if valOffical != valAny {
					t.Fatalf("unexpected value obtained; got %q want %q", valAny, valOffical)
				}
			}
			wgGetAfterDeleted.Done()
		}(i, &wgGetAfterDeleted)
	}
	wgGetAfterDeleted.Wait()
	log.Println("清除官方map前GC耗时")
	gcRunAndPrint()
	mapOffical = nil
	log.Println("清除官方map后GC耗时")
	gcRunAndPrint()
	log.Println("开始测试同时并发读写删,因为读写锁耗时会比较长")
	//边写，边删
	var wgSetGetDelete sync.WaitGroup
	wgSetGetDelete.Add(chanNum)
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := 0; i < max+100; i++ {
				mapAny.SetString(strconv.Itoa(i), strconv.Itoa(i))
				mapAny.GetString(strconv.Itoa(i - 1))
				mapAny.DeleteString(strconv.Itoa(i - 1))
			}
			wgSetGetDelete.Done()
		}(i, &wgSetGetDelete)
	}
	wgSetGetDelete.Wait()
}

//一般是第二次执行才是真实耗时
func gcRunAndPrint() {
	runtime.GC()
	debug.FreeOSMemory()
	begin_time := time.Now()
	runtime.GC()
	debug.FreeOSMemory()
	log.Println("GC耗时:", "耗时:", time.Now().Sub(begin_time).String())
}
