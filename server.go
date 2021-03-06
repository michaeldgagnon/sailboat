package sailboat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"github.com/hashicorp/raft"
)

//=====================================================================
// Helpers
//=====================================================================
// Join the cluster as the raftAddr by negotiating with the existing
// peer at joinAddr. If the target peer is not the leader, it will
// forward us to who it thinks is the leader. This will recursively
// follow those forwards (and block) until a join succeeds or gets an error 
func join(joinAddr, raftAddr string) error {
	// Trim any path prefix that may have been added into a location redirect
	if (strings.HasPrefix(joinAddr, "/")) {
		joinAddr = joinAddr[1:]
	}
	
	// Ask the target node to join the cluster
	b, err := json.Marshal(map[string]string{"addr": raftAddr})
	if err != nil {
		return err
	}
	rsp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer rsp.Body.Close()
	
	// If the node told us the leader is somewhere else, go try that place
	if (rsp.StatusCode == http.StatusUseProxy) {
		return join(rsp.Header.Get("Location"), raftAddr)
	}

	// Either we're in, or we failed
	if (rsp.StatusCode != http.StatusOK) {
		return errors.New("Failed to join cluster")
	}
	return nil
}

//=====================================================================
// Exports
//=====================================================================
// Enter into the cluster and get back a reference to it
// If a join address is provided, then this call blocks until the join resolves
func Start(name string, raftBind string, peerJoin string, data raft.FSM) (*Cluster, error) {
	// The leader service binds to the same ip as raft, but at a constant port offset
	leaderBind, err := raftBindToLeaderBind(raftBind)
	if (err != nil) {
		return nil, fmt.Errorf("failed to parse raft binding: %s", err.Error())
	}
	
	dirName := raftBind
	dirName = strings.Replace(dirName, ":", "_", -1)
	dirName = strings.Replace(dirName, ".", "_", -1)
	raftDir := fmt.Sprintf("raft/%s/%s", name, dirName)
	os.MkdirAll(raftDir, 0700)

	// Build cluster state
	cluster := newCluster(raftDir, raftBind, data)
	if err := cluster.start(peerJoin == ""); err != nil {
		return nil, fmt.Errorf("failed to start cluster: %s", err.Error())
	}

	// Listen for leader API interactions
	leaderApi := newLeaderService(leaderBind, cluster)
	if err := leaderApi.start(); err != nil {
		return nil, fmt.Errorf("failed to listen on leader api: %s", err.Error())
	}

	// If join was specified, make the join request.
	if peerJoin != "" {
		peerJoin, err := raftBindToLeaderBind(peerJoin)
		if (err != nil) {
			return nil, fmt.Errorf("failed to parse peer join: %s", err.Error())
		}
		
		if err := join(peerJoin, raftBind); err != nil {
			return nil, fmt.Errorf("failed to join %s: %s", peerJoin, err.Error())
		}
	}

	return cluster, nil
}