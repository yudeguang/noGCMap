// Copyright 2020 rateLimit Author(https://github.com/yudeguang/noGCMap). All Rights Reserved.
//
// This Source Code Form is subject to the terms of the MIT License.
// If a copy of the MIT was not distributed with this file,
// You can obtain one at https://github.com/yudeguang/noGCMap.
package noGCMap

import (
	"log"
	"strconv"
	"sync"
	"testing"
)

func TestInt(t *testing.T) {
	log.SetFlags(log.Lshortfile | log.Ltime)
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
	var mapOffical = make(map[int]string)
	var mapInt = NewInt()
	log.Println("开始测试并发写")
	//set
	var wgSet sync.WaitGroup
	wgSet.Add(chanNum)
	mapInt.SetString(-10, "")
	mapInt.SetString(-1, bigStr)

	//offical
	for i := 0; i < max; i++ {
		mapOffical[i] = "@" + strconv.Itoa(i) + str
	}
	//mapAny
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := 0; i < max; i++ {
				mapInt.SetString(i, "@"+strconv.Itoa(i)+str)
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
				valOffical, existOffical := mapOffical[i]
				valAny, existAny := mapInt.GetString(i)
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
	valEmpty, exist := mapInt.GetString(-10)
	if !exist {
		t.Fatalf("unexpected value obtained; got %v want %v", exist, true)
	}
	if valEmpty != "" {
		t.Fatalf("unexpected value obtained; got %q want %q", valEmpty, "")
	}

	//算一下key个数
	var keyNum, deleteItemNum int
	for i := range mapInt.buckets {
		keyNum = keyNum + len(mapInt.buckets[i].index)
		deleteItemNum = deleteItemNum + mapInt.buckets[i].deleteItemNum
	}
	keyNum = keyNum + len(mapInt.mapForLongKVPair)

	if keyNum != max+2 {
		t.Fatalf("unexpected value obtained; got %v want %v", keyNum, max+3)
	}
	log.Println("开始测试并发删除")
	//delete
	var wgDelete sync.WaitGroup
	wgDelete.Add(chanNum)
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := -100; i < maxDeleted; i++ {
				mapInt.Delete(i)
				_, exist := mapInt.GetString(i)
				if exist {
					t.Fatalf("unexpected value obtained; got %v want %v", exist, false)
				}
			}
			wgDelete.Done()
		}(i, &wgDelete)
	}
	wgDelete.Wait()
	for i := -100; i < maxDeleted; i++ {
		delete(mapOffical, i)
	}
	log.Println("开始测试并发删除之后与官方结果对比")
	//get 删除之后再用get测试
	var wgGetAfterDeleted sync.WaitGroup
	wgGetAfterDeleted.Add(chanNum)
	for i := 0; i < chanNum; i++ {
		go func(i int, wg *sync.WaitGroup) {
			for i := 0; i < max+100; i++ {
				valOffical, existOffical := mapOffical[i]
				valAny, existAny := mapInt.GetString(i)
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
				mapInt.SetString(i, strconv.Itoa(i))
				mapInt.GetString(i - 1)
				mapInt.Delete(i - 1)
			}
			wgSetGetDelete.Done()
		}(i, &wgSetGetDelete)
	}
	wgSetGetDelete.Wait()
	mapInt = nil
	log.Println("清除所有数据")
	gcRunAndPrint()
	log.Println("开始测试2000W键值对 GC耗时")
	mapint2 := NewInt()
	for i := 0; i < 20000000; i++ {
		mapint2.SetString(i, "@"+strconv.Itoa(i)+str)
	}
	gcRunAndPrint()
}
