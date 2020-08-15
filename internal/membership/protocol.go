package membership

import (
	"math"
	"sync"
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/errors"
	"github.com/nm-morais/go-babel/pkg/logs"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
	"github.com/sirupsen/logrus"
)

const protoID = 1000
const protoName = "DemonTree"

type DemmonTree struct {
	logger *logrus.Logger
	config DemmonTreeConfig

	// node state
	myLevel       uint16
	myParent      *peer.Peer
	parentLatency uint64
	myChildren    []peer.Peer

	// join state
	myPendingChildren map[string]peer.Peer
	children          map[string][]peer.Peer
	parents           map[string]peer.Peer
	parentLatencies   map[string]uint64

	joinLevel uint16

	advanceLevel       *sync.Mutex
	currLevelPeersDone []map[string]peer.Peer
	currLevelPeers     []map[string]peer.Peer

	myLatenciesMutex *sync.RWMutex
	myLatencies      map[string]uint64
}

func NewDemmonTree(config DemmonTreeConfig) protocol.Protocol {
	return &DemmonTree{

		parents:            make(map[string]peer.Peer),
		children:           make(map[string][]peer.Peer),
		advanceLevel:       &sync.Mutex{},
		currLevelPeers:     []map[string]peer.Peer{},
		currLevelPeersDone: []map[string]peer.Peer{},
		joinLevel:          0,
		myChildren:         []peer.Peer{},
		myLatencies:        make(map[string]uint64),
		myLatenciesMutex:   &sync.RWMutex{},
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
	pkg.RegisterTimer(d.ID(), NewJoinTimer(5*time.Second))

	for _, landmark := range d.config.Landmarks {
		if pkg.SelfPeer().Equals(landmark) {
			d.logger.Infof("I am landmark")
			d.myLevel = 0
			d.myParent = nil
			d.myChildren = []peer.Peer{}
			return
		}
	}

	d.currLevelPeers = []map[string]peer.Peer{}
	d.currLevelPeers = append(d.currLevelPeers, make(map[string]peer.Peer))
	d.joinLevel = 1
	for _, landmark := range d.config.Landmarks {
		d.currLevelPeers[d.joinLevel-1][landmark.ToString()] = landmark
		d.parents[landmark.ToString()] = nil
		joinMsg := JoinMessage{}
		d.sendMessageTmpChan(joinMsg, landmark)
	}
}

func (d *DemmonTree) Init() {
	pkg.RegisterMessageHandler(d.ID(), JoinMessage{}, d.handleJoinMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinReplyMessage{}, d.handleJoinReplyMessage)
	pkg.RegisterMessageHandler(d.ID(), JoinAsChildMessage{}, d.handleJoinAsChildMessage)
	pkg.RegisterTimerHandler(d.ID(), joinTimerID, d.handleJoinTimer)
}

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {

}

func (d *DemmonTree) InConnRequested(peer peer.Peer) bool {
	return true
}

func (d *DemmonTree) DialSuccess(sourceProto protocol.ID, peer peer.Peer) bool {
	panic("implement me")
}

func (d *DemmonTree) DialFailed(peer peer.Peer) {
	panic("implement me")
}

func (d *DemmonTree) OutConnDown(peer peer.Peer) {

	panic("implement me")
}

func (d *DemmonTree) MessageDelivered(message message.Message, peer peer.Peer) {

	d.logger.Info("Message %+v delivered to: ", message, peer)
}

func (d *DemmonTree) MessageDeliveryErr(message message.Message, peer peer.Peer, error errors.Error) {
	d.logger.Warn("Message failed to %+v deliver to: ", message, peer)
}

// message handlers

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {
	if d.myLevel == 0 {

		reply := JoinReplyMessage{
			Children:      d.myChildren,
			Level:         d.myLevel,
			ParentLatency: 0,
		}
		d.sendMessageTmpChan(reply, sender)

	} else {
		reply := JoinReplyMessage{
			Children:      d.myChildren,
			Level:         d.myLevel,
			ParentLatency: d.parentLatency,
		}
		d.sendMessageTmpChan(reply, sender)
	}
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(JoinReplyMessage)

	if d.joinLevel != replyMsg.Level {
		// discard old repeated messages
		return
	}
	d.currLevelPeersDone[replyMsg.Level][sender.ToString()] = sender
	d.children[sender.ToString()] = replyMsg.Children
	d.parentLatencies[sender.ToString()] = replyMsg.ParentLatency
	for _, children := range replyMsg.Children {
		d.parents[children.ToString()] = sender
	}

	d.advanceLevel.Lock()
	if d.canProgressToNextLevel() {
		d.progressToNextStep()
	}
	d.advanceLevel.Unlock()
}

func (d *DemmonTree) progressToNextStep() {

	lowestLatencyPeer, peerLat, err := d.getLowestLatencyPeer(d.joinLevel)
	if err != nil {
		d.logger.Panicf(err.Reason())
	}

	if d.myLatencies[d.parents[lowestLatencyPeer.ToString()].ToString()] < peerLat {
		toSend := JoinAsChildMessage{}
		d.sendMessageTmpChan(toSend, d.parents[lowestLatencyPeer.ToString()])
		return
	}

	if d.parentLatencies[lowestLatencyPeer.ToString()]+d.config.GParentLatencyIncreaseThreshold >
		d.myLatencies[d.parents[lowestLatencyPeer.ToString()].ToString()] {

		possibleChildren := make(map[string]peer.Peer, len(d.currLevelPeersDone[d.joinLevel]))
		aux := make([]peer.Peer, 0, len(d.currLevelPeersDone[d.joinLevel]))
		for peerID, p := range d.currLevelPeersDone[d.joinLevel] {
			if d.parentLatencies[peerID]+d.config.GParentLatencyIncreaseThreshold >
				d.myLatencies[d.parents[peerID].ToString()] {
				possibleChildren[peerID] = p
				aux = append(aux, p)
			}
		}

		toSendToChildren := JoinAsParentMessage{
			Children: aux,
			Level:    d.joinLevel,
		}

		for _, possibleChild := range possibleChildren {
			d.sendMessageTmpChan(toSendToChildren, possibleChild)
		}
		d.myPendingChildren = possibleChildren
		d.sendMessageTmpChan(JoinAsChildMessage{}, d.parents[lowestLatencyPeer.ToString()])
		return
	}

	// base case
	if len(d.children[lowestLatencyPeer.ToString()]) == 0 {
		d.sendMessageTmpChan(JoinAsChildMessage{}, lowestLatencyPeer)
		return
	}

	d.joinLevel++
	d.currLevelPeers[d.joinLevel-1] = make(map[string]peer.Peer, len(d.children[lowestLatencyPeer.ToString()]))
	d.currLevelPeersDone[d.joinLevel] = make(map[string]peer.Peer)

	for _, p := range d.children[lowestLatencyPeer.ToString()] {
		d.currLevelPeers[d.joinLevel-1][p.ToString()] = p
		d.sendMessage(JoinMessage{}, p)
	}
}

func (d *DemmonTree) canProgressToNextLevel() bool {
	for _, p := range d.currLevelPeers[d.joinLevel-1] {
		_, ok := d.currLevelPeersDone[d.joinLevel][p.ToString()]
		if !ok {
			return false
		}

		d.myLatenciesMutex.RLock()
		_, ok = d.myLatencies[p.ToString()]
		d.myLatenciesMutex.RUnlock()
		if !ok {
			return false
		}
	}
	return true
}

// aux functions

func (d *DemmonTree) sendMessageTmpChan(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessageSideStream(toSend, destPeer, d.ID(), []protocol.ID{d.ID()}, stream.NewTCPDialer())
}

func (d *DemmonTree) sendMessageAndMeasureLatency(toSend message.Message, destPeer peer.Peer) {
	pkg.SendMessage(toSend, destPeer, d.ID(), []protocol.ID{d.ID()})
	pkg.MeasureLatencyTo(int(d.config.NrSamplesForLatency), destPeer,
		func(p peer.Peer, durations []time.Duration) {
			var total float64 = 0
			for _, duration := range durations {
				total += float64(duration.Nanoseconds())
			}
			d.myLatenciesMutex.Lock()
			d.myLatencies[p.ToString()] = uint64(total / float64(len(durations)))
			d.myLatenciesMutex.Unlock()
			d.advanceLevel.Lock()
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

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {

}
