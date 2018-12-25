package main

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"math/rand"
	"sort"
	"time"
)

// 定义区块结构体
type Block struct {
	Index     int
	Timestamp string
	BPM       int
	Hash      string
	PreHash   string
	Delegate  string
}

// 生成区块函数
func generateBlock(oldBlock Block, _BPM int, address string) (Block, error) {
	var newBlock Block
	t := time.Now()

	newBlock.Index = oldBlock.Index + 1
	newBlock.Timestamp = t.String()
	newBlock.BPM = _BPM
	newBlock.PreHash = oldBlock.Hash
	newBlock.Hash = createBlockHash(newBlock)
	newBlock.Delegate = address

	return newBlock, nil
}

// 生成区块hash
func createBlockHash(block Block) string {
	record := string(block.Index) + block.Timestamp + string(block.BPM) + block.PreHash
	sha3 := sha256.New()
	sha3.Write([]byte(record))
	hash := sha3.Sum(nil)

	return hex.EncodeToString(hash)
}

// 验证区块
func isBlockValid(newBlock, oldBlock Block) bool {
	if oldBlock.Index+1 != newBlock.Index {
		return false
	}
	if newBlock.PreHash != oldBlock.Hash {
		return false
	}

	return true
}

// 区块链
var blockChain []Block

// 超级节点结构体
type Trustee struct {
	name  string
	votes int
}

type trusteeList []Trustee

// 用做排序
func (_trusteeList trusteeList) Len() int {
	return len(_trusteeList)
}

func (_trusteeList trusteeList) Swap(i, j int) {
	_trusteeList[i], _trusteeList[j] = _trusteeList[j], _trusteeList[i]
}

func (_trusteeList trusteeList) Less(i, j int) bool {
	return _trusteeList[j].votes < _trusteeList[i].votes
}

// 选举获得投票数最高的前5节点作为超级节点， 并打乱其顺序
func selectTrustee() []Trustee {
	_trusteeList := []Trustee{
		{"node1", rand.Intn(100)},
		{"node2", rand.Intn(100)},
		{"node3", rand.Intn(100)},
		{"node4", rand.Intn(100)},
		{"node5", rand.Intn(100)},
		{"node6", rand.Intn(100)},
		{"node7", rand.Intn(100)},
		{"node8", rand.Intn(100)},
		{"node9", rand.Intn(100)},
		{"node10", rand.Intn(100)},
		{"node11", rand.Intn(100)},
		{"node12", rand.Intn(100)},
	}

	sort.Sort(trusteeList(_trusteeList))
	result := _trusteeList[:5]
	_trusteeList = result[1:]
	_trusteeList = append(_trusteeList, result[0])
	log.Println("当前超级节点列表时", _trusteeList)

	return _trusteeList
}

func main() {
	t := time.Now()
	// 创建创始区块
	genesisBlock := Block{0, t.String(), 0, createBlockHash(Block{}), "", ""}
	// 只是完成了一次dpos的写区块操作，eos真正的是每个节点实现6个区块，
	// 并且所有超级节点（21个）轮完，之后再做选举
	blockChain = append(blockChain, genesisBlock)
	var trustee Trustee
	for _, trustee = range selectTrustee() {
		_BPM := rand.Intn(100)
		blockHeight := len(blockChain)
		oldBlock := blockChain[blockHeight-1]
		newBlock, err := generateBlock(oldBlock, _BPM, trustee.name)
		if err != nil {
			log.Println(err)
			continue
		}

		if isBlockValid(newBlock, oldBlock) {
			blockChain = append(blockChain, newBlock)
			log.Println("当前操作区块的节点是：", trustee.name)
			log.Println("当前区块数量是：", len(blockChain)-1)
			log.Println("当前区块信息：", blockChain[len(blockChain)-1])
		}
	}
}
