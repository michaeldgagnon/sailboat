package sailboat

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"strconv"
	
	"github.com/hashicorp/raft"
)

//=====================================================================
// Constants
//=====================================================================
const (
	leaderServicePortOffset = 1
)

//=====================================================================
// Types
//=====================================================================
type leaderService struct {
	addr string
	cluster *Cluster
}

//=====================================================================
// Helpers
//=====================================================================
func raftBindToLeaderBind (raftBind string) (string, error) {
	parts := strings.Split(raftBind, ":")
	if (len(parts) != 2) {
		return "", fmt.Errorf("Unexpected format: %s", raftBind)
	}
	
	// If ip is empty, it's loopback
	if (parts[0] == "") {
		parts[0] = "127.0.0.1"
	}
	
	// By convention, assume the peer port is a constant offset from
	// the raft port.
	port, err := strconv.Atoi(parts[1])
	if (err != nil) {
		return "", err
	}
	port += leaderServicePortOffset
	parts[1] = strconv.Itoa(port)
	return fmt.Sprintf("%s:%s", parts[0], parts[1]), nil
}

// Create a new leader service
func newLeaderService(addr string, cluster *Cluster) *leaderService {
	return &leaderService{
		addr:  addr,
		cluster: cluster,
	}
}

//=====================================================================
// leaderService [Private]
//=====================================================================
// Start starts the service.
func (s *leaderService) start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/join", s.handleJoin)
	mux.HandleFunc("/propose", s.handleProposal)
	
	go func() {
		if err := http.ListenAndServe(s.addr, mux); err != nil {
			log.Fatalf("Leader API failed: %s", err)
		}
	}()

	return nil
}

// Handle requests to join the cluster. 
// Either we are the leader and we handle it here, or we are not the leader
// and we redirect them to where we know the leader to be
func (s *leaderService) handleJoin(w http.ResponseWriter, r *http.Request) {
	m := map[string]string{}
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	remoteAddr, ok := m["addr"]
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := s.cluster.join(remoteAddr); err != nil {
		// Already in the cluster? No problem
		if (err == raft.ErrKnownPeer) {
			return
		}
		
		// Any error other than NotLeader is unexpected now
		if (err != raft.ErrNotLeader) {
			w.WriteHeader(http.StatusInternalServerError)
			log.Println("Unexpected join error: ", err.Error())
			return
		}
		
		// Handle NotLeader errors by forwarding to leader
		leader, err := s.cluster.getLeaderServiceAddress()
		if (err != nil) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, leader, http.StatusUseProxy)
	}
}

// Handle requests to propose changes to the FSM
// This must always be received by the leader, or it will fail.
// Proposers must always be active peers in the cluster and thus know the leader
// This request will block until it fails or the proposal is accepted by quorum
func (s *leaderService) handleProposal(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if (err != nil) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.cluster.handleProposal(body)
}