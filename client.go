package main  
  
import (  
    "fmt"  
    "hash/crc32"  
    "sort"     
    "net/http"
    "encoding/json" 
    "io/ioutil"
    "os"
    "strings"
)     

type HCirc []uint32  

type Node struct {  
    Id       int  
    IP       string    
}  

type KeyValuePair struct{
    Key int `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}

type ConsistentHashing struct {  
    Nodes       map[uint32]Node  
    Present   map[int]bool  
    Circle      HCirc  
    
}

func (hc *ConsistentHashing) ReturnIP(node *Node) string {  
    return node.IP 
}  
  
func (hc *ConsistentHashing) Get(key string) Node {  
    hash := hc.GetHash(key)  
    i := hc.SearchNode(hash)  
    return hc.Nodes[hc.Circle[i]]  
}

func CreateNewNode(id int, ip string) *Node {  
    return &Node{  
        Id:       id,  
        IP:       ip,  
    }  
}  

func NConsistentHashing() *ConsistentHashing {  
    return &ConsistentHashing{  
        Nodes:     make(map[uint32]Node),   
        Present: make(map[int]bool),  
        Circle:      HCirc{},  
    }  
}

func (hc *ConsistentHashing) SortCircle() {  
    hc.Circle = HCirc{}  
    for k := range hc.Nodes {  
        hc.Circle = append(hc.Circle, k)  
    }  
    sort.Sort(hc.Circle)  
}  

func (hc *ConsistentHashing) GetHash(key string) uint32 {  
    return crc32.ChecksumIEEE([]byte(key))  
}  

func (hc *ConsistentHashing) SearchNode(hash uint32) int {  
    i := sort.Search(len(hc.Circle), func(i int) bool {return hc.Circle[i] >= hash })  
    if i < len(hc.Circle) {  
        if i == len(hc.Circle)-1 {  
            return 0  
        } else {  
            return i  
        }  
    } else {  
        return len(hc.Circle) - 1  
    }  
}  

func (hc HCirc) Len() int {  
    return len(hc)  
}  
  
func (hc HCirc) Less(i, j int) bool {  
    return hc[i] < hc[j]  
}  
  
func (hc HCirc) Swap(i, j int) {  
    hc[i], hc[j] = hc[j], hc[i]  
}

func (hr *ConsistentHashing) AddNewNode(node *Node) bool {   
    if _, ok := hr.Present[node.Id]; ok {  
        return false  
    }  
    str := hr.ReturnIP(node)  
    hr.Nodes[hr.GetHash(str)] = *(node)
    hr.Present[node.Id] = true  
    hr.SortCircle()  
    return true  
}  

func PutKeyValue(circ *ConsistentHashing, str string, inp string){
        ipAdd := circ.Get(str)  
        add := "http://"+ipAdd.IP+"/keys/"+str+"/"+inp
		fmt.Println(add)
        req,err := http.NewRequest("PUT",add,nil)
        client := &http.Client{}
        resp, err := client.Do(req)
        if err!=nil{
            fmt.Println("Error:",err)
        }else{
            defer resp.Body.Close()
            fmt.Println("PUT Request Done")
        }  
}  

func GetKeyValue(key string,circ *ConsistentHashing){
    var out KeyValuePair 
    ipAdd:= circ.Get(key)
	add := "http://"+ipAdd.IP+"/keys/"+key
	fmt.Println(add)
    resp,err:= http.Get(add)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer resp.Body.Close()
        cont,err:= ioutil.ReadAll(resp.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(cont,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func GetAllKeyValue(add string){
     
    var out []KeyValuePair
    resp,err:= http.Get(add)
    if err!=nil{
        fmt.Println("Error:",err)
    }else{
        defer resp.Body.Close()
        cont,err:= ioutil.ReadAll(resp.Body)
        if(err!=nil){
            fmt.Println(err)
        }
        json.Unmarshal(cont,&out)
        result,_:= json.Marshal(out)
        fmt.Println(string(result))
    }
}

func main() {   
    circ := NConsistentHashing()      
    circ.AddNewNode(CreateNewNode(0, "127.0.0.1:3000"))
	circ.AddNewNode(CreateNewNode(1, "127.0.0.1:3001"))
	circ.AddNewNode(CreateNewNode(2, "127.0.0.1:3002")) 
	if(os.Args[1]=="PUT"){
		key := strings.Split(os.Args[2],"/")
        PutKeyValue(circ,key[0],key[1])
    } else if ((os.Args[1]=="GET") && len(os.Args)==3){
    	GetKeyValue(os.Args[2],circ)
    } else {
		GetAllKeyValue("http://127.0.0.1:3000/keys")
	    GetAllKeyValue("http://127.0.0.1:3001/keys")
	    GetAllKeyValue("http://127.0.0.1:3002/keys")
	}
}  