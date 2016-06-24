package sailboat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"strconv"
	"time"
	
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)


type Cluster struct {
	raftDir string
	raftBind string
	raft *raft.Raft
	data raft.FSM
}

func newCluster (raftDir string, raftBind string, data raft.FSM) *Cluster {
	return &Cluster{
		raftDir: raftDir,
		raftBind: raftBind,
		data: data,
	}
}

func (c *Cluster) join(addr string) error {
	f := c.raft.AddPeer(addr)
	if f.Error() != nil {
		return f.Error()
	}
	return nil
}
	
func (c *Cluster) getLeaderRaftAddress () string {
	return c.raft.Leader()
}

func (c *Cluster) getLeaderPeerAddress () (string, error) {
	leader := c.getLeaderRaftAddress()
	parts := strings.Split(leader, ":")
	if (len(parts) != 2) {
		return "", fmt.Errorf("Unexpected leader format: %s", leader)
	}
	
	// If ip is empty, it's loopback
	if (parts[0] == "") {
		parts[0] = "127.0.0.1"
	}
	
	// By convention, assume the peer port is 1000 less than the raft port
	// We should probably this more intelligently
	port, err := strconv.Atoi(parts[1])
	if (err != nil) {
		return "", err
	}
	port -= 1000
	parts[1] = strconv.Itoa(port)
	return fmt.Sprintf("%s:%s", parts[0], parts[1]), nil
}

func (c *Cluster) start (bootstrap bool) error {
	config := raft.DefaultConfig()

	// Check for any existing peers.
	peers, err := readPeersJSON(filepath.Join(c.raftDir, "peers.json"))
	if err != nil {
		return err
	}

	// Handle bootstrapping of first node
	if bootstrap && len(peers) <= 1 {
		log.Println("BOOTSTRAP")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = true
	}

	// Setup Raft communication.
	addr, err := net.ResolveTCPAddr("tcp", c.raftBind)
	if err != nil {
		return err
	}
	transport, err := raft.NewTCPTransport(c.raftBind, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return err
	}

	// Create stores
	peerStore := raft.NewJSONPeers(c.raftDir, transport)
	snapshots, err := raft.NewFileSnapshotStore(c.raftDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot store: %s", err)
	}
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(c.raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("new bolt store: %s", err)
	}

	// Instantiate Raft
	ra, err := raft.NewRaft(config, c.data, logStore, logStore, snapshots, peerStore, transport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	c.raft = ra
	return nil
}

func readPeersJSON(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		return nil, nil
	}

	var peers []string
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&peers); err != nil {
		return nil, err
	}

	return peers, nil
}

func (c *Cluster) handleProposal (cmd []byte) error {
	if c.raft.State() != raft.Leader {
		return fmt.Errorf("not leader")
	}
	
	f := c.raft.Apply(cmd, raftTimeout)
	if err := f.Error(); err != nil {
		return err
	}
	
	return nil
}

// Propose a command to the cluster. The command is marshalled into json
// The FSM Apply method must later unmarshall the data
// This blocks until the proposal is accepted with quorum or an error occurs
func (c *Cluster) Propose(cmd interface{}) error {
	leader, err := c.getLeaderPeerAddress()
	if (err != nil) {
		return err
	}
	
	b, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	rsp, err := http.Post(fmt.Sprintf("http://%s/cmd", leader), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer rsp.Body.Close()

	if (rsp.StatusCode != http.StatusOK) {
		return fmt.Errorf("Proposal failed: %d", rsp.StatusCode)
	}

	return nil
}