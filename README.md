# noGCMap
https://github.com/yudeguang/noGcMap 与 https://github.com/yudeguang/noGcStaticMap 为同一系列的无GC类型MAP，两者针对的场景有一定差异,noGcStaticMap性能稍高，内存占用更小，但不支持增删改。

对于大型map，比如总数达到千万级别的map,如果键或者值中包含引用类型(string类型，结构体类型，或者任何基本类型+指针的定义 *int, *float 等)，那么这个map在垃圾回收的时候就会非常慢，GC的周期回收时间可以达到秒级甚至分钟级。

对此参考fastcache等，把复杂的不利于GC的复杂map转化为基础类型的map map[uint64]uint32 用于存储索引 和 []byte用于存储实际键值。如此改造之后，基本上实现了零GC,总体而言：

优点:

1)几乎零GC;

2)无hash碰撞问题;

3)支持增删改( 相比 https://github.com/yudeguang/noGcStaticMap );

4)代码量非常少，适合根据自己需求做二次修改;

缺点:
1)性能比 https://github.com/yudeguang/noGcStaticMap 稍差，内存占用稍高;


```go
package main

import (
	"github.com/yudeguang/noGcMap"
	"log"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ltime)
	m := noGCMap.New()
	m.SetString("1", "1原始")
	log.Println(m.GetString("1"))
	m.DeleteString("1")
	log.Println(m.GetString("1"))
	m.SetString("1","1修改")
	log.Println(m.GetString("1"))
	m.SetString("1","1修改2")
	log.Println(m.GetString("1"))
}
```
