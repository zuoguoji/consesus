package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew" //漂亮的格式打印到终端
	"github.com/joho/godotenv"        //也许从之前创建的.evn文件读取配置
)

// 区块定义
type Block struct {
	Index     int // 区块高度
	TimeStamp string
	BPM       int // 脉搏
	Hash      string
	PreHash   string
	Validator string
}

var Blockchain []Block
var tempBlocks []Block // 临时存储，被选区块临时存储这里

var candidateBlocks = make(chan Block) // Block通道，新块都发送到该通道
var announcements = make(chan string)  // 向所有节点广播最新区块

var mutex = &sync.Mutex{}

var validators = make(map[string]int) // 保存每个节点持有令牌数

func main() {
	err := godotenv.Load() // 读取环境变量
	if err != nil {
		log.Fatal(err)
	}
	// 生成创世区块
	t := time.Now()
	genesisBlock := Block{}
	genesisBlock = Block{0, t.String(), 0, calculateBlockHash(genesisBlock), "", ""}
	spew.Dump(genesisBlock)
	Blockchain = append(Blockchain, genesisBlock)

	httpPort := os.Getenv("PORT") // 设置监听端口

	server, err := net.Listen("tcp", ":"+httpPort) // 启动http服务器，开启监听
	if err != nil {
		log.Fatal(err)
	}
	log.Println("HTTP Server Listening on port: ", httpPort)
	defer server.Close()
	// 将候选区块加入到tempBlocks中
	go func() {
		for candidate := range candidateBlocks {
			mutex.Lock()
			tempBlocks = append(tempBlocks, candidate)
			mutex.Unlock()
		}
	}()
	// 选出胜利者
	go func() {
		for {
			pickWinner()
		}
	}()
	// 不断接收请求，处理请求
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Fatal(err)
		}
		go handleConn(conn)
	}
}

func pickWinner() {
	time.Sleep(30 * time.Second) // 每三十秒执行一次
	mutex.Lock()
	temp := tempBlocks
	mutex.Unlock()

	lotterPool := []string{} // 乐透池
	// 对于提议块的暂存区域，我们会通过if len(temp) > 0来判断是否已经有了被提议的区块。
	if len(temp) > 0 {
		// 稍微针对传统的proof of stake算法修改了一下
		// 所有的节点都需要提交一个区块，区块的权重由节点下注的token数量决定
		// 传统的proof of stake算法，验证者节点无需铸造新的区块也能获得收益
	OUTER:
		for _, block := range temp {
			for _, node := range lotterPool {
				// 如果已经在乐透池中，忽略节点
				if block.Validator == node {
					continue OUTER
				}
			}
			// 锁定节点列表
			mutex.Lock()
			setValidators := validators
			mutex.Unlock()
			// 首先，用验证者的令牌填充lotteryPool数组，例如一个验证者有100个令牌，那么在lotteryPool中就将有100个元素填充
			// 如果有一个令牌，那么将仅填充一个元素
			k, ok := setValidators[block.Validator] // k为验证者节点的token数量
			if ok {
				for i := 0; i < k; i++ {
					lotterPool = append(lotterPool, block.Validator)
				}
			}
		}

	}
	// 从乐透池中随机选择一个胜者
	s := rand.NewSource(time.Now().Unix())
	r := rand.New(s)
	lotterWinner := lotterPool[r.Intn(len(lotterPool))]
	// 将胜者提交的区块添加到区块链中，然后让其他节点都知道
	for _, block := range temp {
		if block.Validator == lotterWinner {
			mutex.Lock()
			Blockchain = append(Blockchain, block)
			mutex.Unlock()
			for _ = range validators {
				announcements <- "\nwinning validator: " + lotterWinner + "\n"
			}
			break
		}
	}

	mutex.Lock()
	tempBlocks = []Block{}
	mutex.Unlock()
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	// 接收并打印出来自TCP服务器的任何通知
	go func() {
		for {
			msg := <-announcements
			io.WriteString(conn, msg)
		}
	}()
	// 验证者节点的地址
	var address string
	// 允许用户可以分配一定数量的token来下注
	// token数量越多，就有越大的概率获得新的区块的奖励
	io.WriteString(conn, "Enter token balance: ")
	scanBalance := bufio.NewScanner(conn)
	for scanBalance.Scan() {
		balance, err := strconv.Atoi(scanBalance.Text()) // 字符串转整数
		if err != nil {
			log.Printf("%v not a number: %v", scanBalance.Text(), err)
			return
		}
		t := time.Now()
		address = calculateHash(t.String())
		validators[address] = balance
		fmt.Println(validators)
		break
	}

	io.WriteString(conn, "Enter a new BPM: ") // 输入要上链的心率
	scanBPM := bufio.NewScanner(conn)

	go func() {
		for {
			// 从stdin获取心率值，然后完成一些校验并将其加入到区块链上
			for scanBPM.Scan() {
				bpm, err := strconv.Atoi(scanBPM.Text())
				// 如果有恶意节点想要使用一个有问题的输入来破坏区块链，将这个恶意节点从validators中删除掉,
				// 他们也将丢失他们持有的token。
				if err != nil {
					log.Printf("%v not a number: %v", scanBPM.Text(), err)
					delete(validators, address)
					conn.Close()
				}
				mutex.Lock()
				oldLastIndex := Blockchain[len(Blockchain)-1]
				mutex.Unlock()
				// 铸造一个候选区块
				newBlock, err := generateBlock(oldLastIndex, bpm, address)
				if err != nil {
					log.Println(err)
					continue
				}
				if isBlockValid(newBlock, oldLastIndex) {
					candidateBlocks <- newBlock
				}
				io.WriteString(conn, "\nEnter a new BPM: ")
			}
		}
	}()
	// 模拟接收到广播
	// 周期性的打印出最新的区块链，这样每个验证者都能获知最新的状态
	for {
		time.Sleep(time.Minute)
		mutex.Lock()
		output, err := json.Marshal(Blockchain)
		mutex.Unlock()
		if err != nil {
			log.Fatal(err)
		}
		// 写入conn中，客户端打印
		io.WriteString(conn, string(output)+"\n")
	}
}

// 校验区块合法性
func isBlockValid(newBlock, oldBlock Block) bool {
	if oldBlock.Index+1 != newBlock.Index {
		return false
	}
	if oldBlock.Hash != newBlock.PreHash {
		return false
	}
	if calculateBlockHash(newBlock) != newBlock.Hash {
		return false
	}
	return true
}

// 计算hash函数
func calculateHash(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	hashed := h.Sum(nil)
	return hex.EncodeToString(hashed)
}

// 计算hash
func calculateBlockHash(block Block) string {
	record := string(block.Index) + block.TimeStamp + string(block.BPM) + block.PreHash
	return calculateHash(record)
}

// 产生新的区块，需要使用上一个区块的哈希
func generateBlock(oldBlock Block, BPM int, address string) (Block, error) {
	var newBlock Block
	t := time.Now()

	newBlock.Index = oldBlock.Index + 1
	newBlock.TimeStamp = t.String()
	newBlock.BPM = BPM
	newBlock.PreHash = oldBlock.Hash
	newBlock.Hash = calculateBlockHash(newBlock)
	newBlock.Validator = address

	return newBlock, nil
}
