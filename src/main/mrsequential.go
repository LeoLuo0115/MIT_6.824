package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.5840/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key. 
// 定义一个切片类型，用于排序
type ByKey []mr.KeyValue

// for sorting by key.
// 实现sort.Interface接口的三个方法, 这样可以使用sort.Sort()方法进行排序
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	// 传入的参数必须大于3，第一个参数是mrsequential.go，第二个参数是wc.go，第三个参数是输入文件 pg*.txt
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}

	// 加载插件，从 wc.go 获取map和reduce函数
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	
	// 创建一个切片，用于存储map的输出
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	for _, kv := range intermediate {
		fmt.Fprintln(ofile, kv.Key, kv.Value)
	}
	fmt.Fprintln(ofile, "------------------")
	
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	
	// reduce的输入是一个key对应的所有value，输出是一个string
	// 记住我们 intermediate 是按照key排序的
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 这个循环的目的是找到所有具有相同 Key 的元素。
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		// 从 i 到 j - 1，将 intermediate[k].Value 添加到 values 中。这样，values 就包含了所有具有相同 Key 的元素的 Value。
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		
		// 调用 reduce 函数，计算的是在 intermediate 中所有具有相同 Key 的元素出现的次数。
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
