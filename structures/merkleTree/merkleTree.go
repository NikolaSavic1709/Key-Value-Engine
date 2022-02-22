package merkleTree

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"io/ioutil"
	"os"
	"strings"
)

type MerkleTree struct {
	root *Node
}

func (merkle *MerkleTree) String() string {
	return merkle.root.String()
}

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

// FormLeaf pravi list cvor na osnovu podatka
func FormLeaf(data []byte) Node {
	h := Hash(data)
	return Node{
		data:  h,
		left:  nil,
		right: nil,
	}
}

// FormLeaves pravi listove merkle stabla od niza podataka
func (merkle *MerkleTree) FormLeaves(dataBlock [][]byte) []*Node {
	leaves := make([]*Node, 0, 1)
	for _, data := range dataBlock {
		newNode := FormLeaf(data)
		leaves = append(leaves, &newNode)
	}
	return leaves
}

func (merkle *MerkleTree) Build(dataBlock [][]byte) *Node {
	leaves := merkle.FormLeaves(dataBlock)

	merkle.BuildTree(leaves)
	return merkle.root
}

func (merkle *MerkleTree) BuildTree(row []*Node) []*Node {
	newRow := make([]*Node, 0, 1)

	if len(row) == 0 {
		panic("Greska, jedan red stabla nema elemente")
	}

	if len(row)%2 != 0 {
		h := Hash(make([]byte, 0, 0))
		newNode := Node{
			data:  h,
			left:  nil,
			right: nil,
		}
		row = append(row, &newNode)
	}

	for i := 0; i < len(row); i += 2 {
		lefty := row[i]
		righty := row[i+1]
		var newData []byte
		newData = lefty.data[:]
		newData = append(newData, righty.data[:]...)
		newNode := Node{
			data:  Hash(newData),
			left:  lefty,
			right: righty,
		}
		newRow = append(newRow, &newNode)
	}
	if len(newRow) == 1 {
		merkle.root = newRow[0]
		return newRow
	} else if len(newRow) > 1 {
		return merkle.BuildTree(newRow)
	} else {
		panic("Greska!")
	}
}

func (merkle *MerkleTree) Serialize(merkleFilePath string) {
	file, err := os.OpenFile(merkleFilePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	queue := make([]*Node, 0, 1)
	queue = append(queue, merkle.root)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if node.left != nil {
			queue = append(queue, node.left)
		}
		if node.right != nil {
			queue = append(queue, node.right)
		}

		_, err := writer.Write([]byte(node.String() + ";"))
		if err != nil {
			panic(err)
		}
	}
	writer.Flush()
}

func (merkle *MerkleTree) Deserialize(merkleFilePath string) {

	file, err := os.OpenFile(merkleFilePath, os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}

	allData, err := ioutil.ReadAll(file)
	if err != nil {
		panic(err)
	}

	strHashes := strings.Split(string(allData), ";")
	strHashes = strHashes[:len(strHashes)-1]

	file.Close()

	nodes := make([]Node, len(strHashes), len(strHashes))

	for i := 0; i < len(strHashes); i++ {
		decoded, _ := hex.DecodeString(strHashes[i])

		var newData [20]byte
		for j := 0; j < 20; j++ {
			newData[j] = decoded[j]
		}

		nodes[i] = Node{
			data:  newData,
			left:  nil,
			right: nil,
		}
	}

	if len(nodes) == 0 {
		merkle.root = nil
		return
	}

	queue := make([]*Node, 0, 1)
	i := 0
	merkle.root = &nodes[i]
	i++
	queue = append(queue, merkle.root)

	for len(queue) != 0 {
		node := queue[0]
		queue = queue[1:]

		if i >= len(queue) {
			break
		}
		node.left = &nodes[i]
		queue = append(queue, node.left)
		i++

		if i >= len(queue) {
			break
		}
		node.right = &nodes[i]
		i++
		queue = append(queue, node.right)
	}
}

//func main() {
//	MyMerkleTree := MerkleTree{}
//	dataBlock := make([][]byte, 0, 1)
//	dataBlock = append(dataBlock, []byte("Miki"))
//	dataBlock = append(dataBlock, []byte("Milane"))
//	dataBlock = append(dataBlock, []byte("Sto"))
//	dataBlock = append(dataBlock, []byte("Mi"))
//	dataBlock = append(dataBlock, []byte("Ne"))
//	dataBlock = append(dataBlock, []byte("Das"))
//	dataBlock = append(dataBlock, []byte("Mira"))
//
//	rootHash := MyMerkleTree.Build(dataBlock)
//	fmt.Println(rootHash.String())
//	fmt.Println(MyMerkleTree.root.String())
//	fmt.Println(MyMerkleTree.root.left.String())
//	fmt.Println(MyMerkleTree.root.right.right.String())
//
//	MyMerkleTree.Serialize()
//	NewMerkleTree := MerkleTree{}
//	NewMerkleTree.Deserialize()
//
//	fmt.Println(NewMerkleTree.root.String())
//}
