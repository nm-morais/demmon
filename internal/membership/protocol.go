package membership

import (
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const protoName = "DemonTree"

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	myLevel       uint16
	myGrandParent peer.Peer
	myParent      peer.Peer
	parentLatency uint64
	myChildren    map[string]peer.Peer

	// join state

	children        map[string]map[string]peer.Peer
	parents         map[string]peer.Peer
	parentLatencies map[string]uint64

	myPendingChildren map[string]peer.Peer
	myPendingParent   peer.Peer

	joinLevel uint16

	advanceLevel       *sync.Mutex
	currLevelPeersDone []map[string]peer.Peer
	currLevelPeers     []map[string]peer.Peer

	retries     map[string]int
	myLatencies map[string]uint64
}

func NewDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{

		parents:            make(map[string]peer.Peer),
		children:           make(map[string]map[string]peer.Peer),
		advanceLevel:       &sync.Mutex{},
		currLevelPeers:     []map[string]peer.Peer{},
		currLevelPeersDone: []map[string]peer.Peer{},
		myPendingChildren:  map[string]peer.Peer{},
		myPendingParent:    nil,
		joinLevel:          1,
		retries:            map[string]int{},
		myChildren:         map[string]peer.Peer{},
		myLatencies:        make(map[string]uint64),
		myParent:           nil,
		parentLatencies:    make(map[string]uint64),
		logger:             logs.NewLogger(protoName),
		config:             config,
	}
}

func (d *DemmonTree) ID() protocol.ID {
	return protoID
}

func (d *DemmonTree) Name() string {
	return protoName
}

func (d *DemmonTree) Logger() *logrus.Logger {
	return d.logger
}

func (d *DemmonTree) Start() {
	for _, landmark := range d.config.Landmarks {
		if pkg.SelfPeer().Equals(landmark) {
			d.logger.Infof("I am landmark")
			d.myLevel = 0
			d.myParent = nil
			d.myChildren = map[string]peer.Peer{}
			return
		}
	}
	pkg.RegisterTimer(d.ID(), NewJoinTimer(10*time.Second))
	d.joinOverlay()
}

func (d *DemmonTree) Init() {
	pkg.RegisterMessageHandler(d.ID(), JoinMessage{}, d.handleJoinMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinReplyMessage{}, d.handleJoinReplyMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinAsChildMessage{}, d.handleJoinAsChildMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinAsParentMessage{}, d.handleJoinAsParentMessage)
	pkg.RegisterMessageHandler(d.ID(), UpdateParentMessage{}, d.handleUpdateParentMessage)
	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
	pkg.RegisterTimerHandler(d.ID(), parentRefreshTimerID, d.handleRefreshParentTimer)

}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {

	if d.myPendingParent != nil && d.myPendingParent.ToString() == peer.Addr().String() {
		pkg.Dial(peer, d.ID(), stream.NewTCPDialer())
		return true
	}

	if d.myParent != nil && d.myParent.ToString() == peer.Addr().String() {
		pkg.Dial(peer, d.ID(), stream.NewTCPDialer())
		return true
	}

	_, isChildren := d.myChildren[peer.ToString()]
	if isChildren {
		return true
	}

	if d.myPendingParent == nil {
		d.logger.Infof("d.myPendingParent: %+v", d.myPendingParent)
	} else {
		d.logger.Infof("d.myPendingParent: %s", d.myPendingParent.ToString())
	}
	d.logger.Infof("d.myChildren: %+v", d.myChildren)

	d.logger.Panicf("Was dialed by unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {

	if sourceProto != d.ID() {
		return false
	}

	if d.myPendingParent != nil && d.myPendingParent.ToString() == peer.Addr().String() {
		d.myPendingParent = nil
		d.myParent = peer
		return true
	}

	_, isPendingChildren := d.myPendingChildren[peer.ToString()]
	if isPendingChildren {
		delete(d.myPendingChildren, peer.ToString())
		pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(1*time.Second, peer))
		d.myChildren[peer.ToString()] = peer
		d.logger.Infof("I have new children %s", peer.ToString())
		return true
	}

	d.logger.Infof("d.myPendingParent: %s", d.myPendingParent)
	d.logger.Infof("d.myChildren: %+v", d.myChildren)
	d.logger.Panicf("Dialed unknown peer: %s", peer.ToString())
	return false
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	d.logger.Errorf("Failed to dial %s", peer.ToString())
}

func (d *DemmonTree) OutConnDown(peer peer.Peer) {
	// TODO
	d.logger.Errorf("Peer down %s", peer.ToString())
}

func (d *DemmonTree) MessageDelivered(message message.Message, peer peer.Peer) {
	d.logger.Infof("Message %s delivered to: %s", reflect.TypeOf(message), peer.ToString())
}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	d.logger.Infof("Message %s failed to deliver to: %s because: %s", reflect.TypeOf(message), peer.ToString(), error.Reason())
	switch message.(type) {
	case JoinMessage:
		_, ok := d.retries[peer.ToString()]
		if !ok {
			d.retries[peer.ToString()] = 0
		}
		d.retries[peer.ToString()]++
		if d.retries[peer.ToString()] >= d.config.MaxRetriesJoinMsg {
			d.logger.Warnf("Deleting peer %s from currLevelPeers because it exceeded max retries (%d)", peer.ToString(), d.config.MaxRetriesJoinMsg)
			delete(d.currLevelPeers[d.joinLevel-1], peer.ToString())
			delete(d.retries, peer.ToString())
			return
		}
		d.sendMessageAndMeasureLatency(message, peer)
	}
}

func (d *DemmonTree) joinOverlay() {
	nrLandmarks := len(d.config.Landmarks)
	d.currLevelPeersDone = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)} // start level 1
	d.currLevelPeers = []map[string]peer.Peer{make(map[string]peer.Peer, nrLandmarks)}     // start level 1
	d.joinLevel = 1
	d.logger.Infof("Landmarks:")
	for i, landmark := range d.config.Landmarks {
		d.logger.Infof("%d :%s", i, landmark.ToString())
		d.currLevelPeers[0][landmark.ToString()] = landmark
		d.parents[landmark.ToString()] = nil
		joinMsg := JoinMessage{}
		d.sendMessageAndMeasureLatency(joinMsg, landmark)
	}
}

func (d *DemmonTree) progressToNextStep() {

	d.logger.Infof("Progressing to next step")

	lowestLatencyPeer, peerLat, err := d.getLowestLatencyPeer(d.joinLevel - 1)
	if err != nil {
		d.logger.Panicf(err.Reason())
	}

	d.logger.Infof("Lowest Latency Peer: %s , Latency: %d", lowestLatencyPeer.ToString(), peerLat)

	if d.joinLevel > 1 && d.parentLatencies[lowestLatencyPeer.ToString()]+d.config.GParentLatencyIncreaseThreshold >
		d.myLatencies[d.parents[lowestLatencyPeer.ToString()].ToString()] {

		aux := make([]peer.Peer, 0, len(d.currLevelPeersDone[d.joinLevel]))

		for peerID, p := range d.currLevelPeersDone[d.joinLevel] {
			if d.parentLatencies[peerID]+d.config.GParentLatencyIncreaseThreshold >
				d.myLatencies[d.parents[peerID].ToString()] {
				aux = append(aux, p)
			}
		}

		toSendToChildren := JoinAsParentMessage{
			Children: aux,
			Level:    d.joinLevel,
		}

		for _, possibleChild := range aux {
			d.myPendingChildren[possibleChild.ToString()] = possibleChild
			d.sendMessageTmpChan(toSendToChildren, possibleChild)
		}
		d.myPendingParent = d.parents[lowestLatencyPeer.ToString()]
		d.sendMessageTmpChan(JoinAsChildMessage{}, d.parents[lowestLatencyPeer.ToString()])
		return
	}

	if d.joinLevel > 1 && d.myLatencies[d.parents[lowestLatencyPeer.ToString()].ToString()] < peerLat {
		toSend := JoinAsChildMessage{}
		d.myPendingParent = d.parents[lowestLatencyPeer.ToString()]
		d.sendMessageTmpChan(toSend, d.parents[lowestLatencyPeer.ToString()])
		return
	}

	// base case
	if len(d.children[lowestLatencyPeer.ToString()]) == 0 {
		d.logger.Infof("Pending parent: %s", lowestLatencyPeer.ToString())
		d.myPendingParent = lowestLatencyPeer
		d.sendMessageTmpChan(JoinAsChildMessage{}, lowestLatencyPeer)
		return
	}

	d.joinLevel++
	d.currLevelPeers[d.joinLevel-1] = make(map[string]peer.Peer, len(d.children[lowestLatencyPeer.ToString()]))
	d.currLevelPeersDone[d.joinLevel-1] = make(map[string]peer.Peer)

	for _, p := range d.children[lowestLatencyPeer.ToString()] {
		d.currLevelPeers[d.joinLevel-1][p.ToString()] = p
		d.sendMessage(JoinMessage{}, p)
	}
}

func (d *DemmonTree) canProgressToNextLevel() (res bool) {

	d.logger.Info("d.currLevelPeersDone:")
	for i, peersDone := range d.currLevelPeersDone {
		d.logger.Infof("Level %d", i)
		for _, peerDone := range peersDone {
			d.logger.Infof("%s", peerDone.ToString())
		}
	}

	d.logger.Info("d.currLevelPeers:")
	for i, peersDone := range d.currLevelPeers {
		d.logger.Infof("Level %d", i)
		for _, peerDone := range peersDone {
			d.logger.Infof("%s", peerDone.ToString())
		}
	}

	defer d.logger.Infof("Can progress? %t", res)
	if len(d.currLevelPeers) == 0 {
		d.logger.Info("Cannot progress because len(d.currLevelPeers) == 0")
		res = false
		return
	}

	if len(d.currLevelPeers[d.joinLevel-1]) == 0 {
		d.logger.Infof("Cannot progress because len(d.currLevelPeers[d.joinLevel-1]) == 0 ")
		res = false
		return
	}

	for _, p := range d.currLevelPeers[d.joinLevel-1] {
		_, ok := d.currLevelPeersDone[d.joinLevel-1][p.ToString()]
		if !ok {
			d.logger.Infof("Cannot progress because not all peers are done, missing: %s", p.ToString())
			res = false
			return
		}

		_, ok = d.myLatencies[p.ToString()]
		if !ok {
			res = false
			d.logger.Infof("Cannot progress because missing latency measurent from: %s", p.ToString())
			return
		}
	}
	res = true
	return
}

// aux functions

func (d *DemmonTree) sendMessageTmpChan(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessageSideStream(toSend, destPeer, d.ID(), []protocol.ID{d.ID()}, stream.NewTCPDialer())
}

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer peer.Peer) {
	d.sendMessageTmpChan(toSend, destPeer)
	pkg.MeasureLatencyTo(int(d.config.NrSamplesForLatency), destPeer,
		func(p peer.Peer, durations []time.Duration) {
			var total float64 = 0
			for _, duration := range durations {
				total += float64(duration.Nanoseconds())
			}
			d.logger.Infof("Latency to %s: %d nano (%d measurements)", destPeer.ToString(), uint64(total/float64(len(durations))), len(durations))
			d.advanceLevel.Lock()
			d.myLatencies[p.ToString()] = uint64(total / float64(len(durations)))
			if d.canProgressToNextLevel() {
				d.progressToNextStep()
			}
			d.advanceLevel.Unlock()
		})
}

func (d *DemmonTree) sendMessage(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessage(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
}

func (d *DemmonTree) getPeersWithLatencyUnder(level uint16, threshold uint64, exclusions ...peer.Peer) []peer.Peer {
	toReturn := make([]peer.Peer, 0, len(d.currLevelPeersDone[level]))
	added := 0
	for _, currPeer := range d.currLevelPeersDone[level] {

		latency, ok := d.myLatencies[currPeer.ToString()]
		if !ok {
			d.logger.Panicf("Do not have latency measurement for %s", currPeer.ToString())
		}

		if latency < threshold {
			excluded := false
			for _, exclusion := range exclusions {
				if exclusion.Equals(currPeer) {
					excluded = true
					break
				}
			}
			if !excluded {
				toReturn[added] = currPeer
				added++
			}
		}
	}
	return toReturn
}

func (d *DemmonTree) getLowestLatencyPeer(level uint16) (peer.Peer, uint64, errors.Error) {
	var lowestLat uint64 = math.MaxUint64
	var lowestLatPeer peer.Peer

	if len(d.currLevelPeersDone[level]) == 0 {
		return nil, math.MaxUint64, errors.NonFatalError(404, "peer collection is empty", protoName)
	}

	for _, currPeer := range d.currLevelPeersDone[level] {

		latency, ok := d.myLatencies[currPeer.ToString()]
		if !ok {
			d.logger.Panicf("Do not have latency measurement for %s", currPeer.ToString())
		}

		if latency < lowestLat {
			lowestLatPeer = currPeer
		}
	}

	return lowestLatPeer, lowestLat, nil
}
