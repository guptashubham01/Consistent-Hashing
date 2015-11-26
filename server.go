package main

import  (
  "github.com/julienschmidt/httprouter"
  "fmt"
  "net/http"
  "strconv"
  "encoding/json"
  "strings"
  "sort"
)

type KeyValuePair struct{
  Key int `json:"key,omitempty"`
  Value string  `json:"value,omitempty"`
} 

var n1,n2,n3 [] KeyValuePair
var idx1,idx2,idx3 int
type KeyPair []KeyValuePair
func (a KeyPair) Len() int           { return len(a) }
func (a KeyPair) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyPair) Less(i, j int) bool { return a[i].Key < a[j].Key }


func GetAllKeysValue(rw http.ResponseWriter, req *http.Request,p httprouter.Params){
  port := strings.Split(req.Host,":")
  if(port[1]=="3000"){
    sort.Sort(KeyPair(n1))
    res,_:= json.Marshal(n1)
    fmt.Fprintln(rw,string(res))
  }else if(port[1]=="3001"){
    sort.Sort(KeyPair(n2))
    res,_:= json.Marshal(n2)
    fmt.Fprintln(rw,string(res))
  }else{
    sort.Sort(KeyPair(n3))
    res,_:= json.Marshal(n3)
    fmt.Fprintln(rw,string(res))
  }
}

func PutKeysValue(rw http.ResponseWriter, req *http.Request,p httprouter.Params){
  port := strings.Split(req.Host,":")
  key,_ := strconv.Atoi(p.ByName("key_id"))
  if(port[1]=="3000"){
    n1 = append(n1,KeyValuePair{key,p.ByName("value")})
    idx1++
  }else if(port[1]=="3001"){
    n2 = append(n2,KeyValuePair{key,p.ByName("value")})
    idx2++
  }else{
    n3 = append(n3,KeyValuePair{key,p.ByName("value")})
    idx3++
  } 
}

func GetKeyValue(rw http.ResponseWriter, req *http.Request,p httprouter.Params){ 
  out := n1
  ind := idx1
  port := strings.Split(req.Host,":")
  if(port[1]=="3001"){
    out = n2 
    ind = idx2
  }else if(port[1]=="3002"){
    out = n3
    ind = idx3
  } 
  key,_ := strconv.Atoi(p.ByName("key_id"))
  for i:=0 ; i< ind ;i++{
    if(out[i].Key==key){
      res,_:= json.Marshal(out[i])
      fmt.Fprintln(rw,string(res))
    }
  }
}

func main(){
  idx1 = 0
  idx2 = 0
  idx3 = 0
  mux := httprouter.New()
    mux.GET("/keys",GetAllKeysValue)
    mux.GET("/keys/:key_id",GetKeyValue)
    mux.PUT("/keys/:key_id/:value",PutKeysValue)
    go http.ListenAndServe(":3000",mux)
    go http.ListenAndServe(":3001",mux)
    go http.ListenAndServe(":3002",mux)
    select {}
}