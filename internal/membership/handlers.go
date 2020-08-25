package membership

import (
	"time"

	"github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	"github.com/nm-morais/go-babel/pkg/stream"
	"github.com/nm-morais/go-babel/pkg/timer"
)

// timer handlers

func (d *DemmonTree) handleJoinTimer(joinTimer timer.Timer) {
	if d.joinLevel == 1 && len(d.currLevelPeers[d.joinLevel-1]) == 0 {
		d.logger.Info("-------------Rejoining overlay---------------")
		d.joinOverlay()
	}
	pkg.RegisterTimer(d.ID(), NewJoinTimer(10*time.Second))
}

func (d *DemmonTree) handleRefreshParentTimer(timer timer.Timer) {
	refreshTimer := timer.(*parentRefreshTimer)
	_, ok := d.myChildren[refreshTimer.Child.ToString()]
	if !ok {
		d.logger.Warnf("Stopped sending refreshParent messages to: %s", refreshTimer.Child.ToString())
		return
	}
	pkg.RegisterTimer(d.ID(), NewParentRefreshTimer(1*time.Second, refreshTimer.Child))
	toSend := UpdateParentMessage{GrandParent: d.myParent, Parent: pkg.SelfPeer(), ParentLevel: d.myLevel}
	d.sendMessage(toSend, refreshTimer.Child)
}

// message handlers

func (d *DemmonTree) handleJoinMessage(sender peer.Peer, msg message.Message) {

	aux := make([]peer.Peer, len(d.myChildren))
	i := 0
	for _, c := range d.myChildren {
		aux[i] = c
		i++
	}

	d.sendMessageTmpTCPChan(JoinReplyMessage{
		Children:      aux,
		Level:         d.myLevel,
		ParentLatency: d.parentLatency,
	}, sender)
}

func (d *DemmonTree) handleJoinReplyMessage(sender peer.Peer, msg message.Message) {
	replyMsg := msg.(JoinReplyMessage)

	d.logger.Infof("Got joinReply: %+v", replyMsg)

	if d.joinLevel-1 != replyMsg.Level {
		d.logger.Warnf("Discarding old message %+v because joinLevel is too high: %d", replyMsg, d.joinLevel)
		// discard old repeated messages
		return
	}

	d.currLevelPeersDone[d.joinLevel-1][sender.ToString()] = sender
	d.children[sender.ToString()] = make(map[string]peer.Peer, len(replyMsg.Children))
	for _, c := range replyMsg.Children {
		d.children[sender.ToString()][c.ToString()] = c
	}

	d.parentLatencies[sender.ToString()] = time.Duration(replyMsg.ParentLatency)
	for _, children := range replyMsg.Children {
		d.parents[children.ToString()] = sender
	}

	if d.canProgressToNextLevel() {
		d.progressToNextStep()
	}
}

func (d *DemmonTree) handleJoinAsParentMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Peer %s wants to be my parent", sender.ToString())
	d.myPendingParent = sender
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handleJoinAsChildMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Peer %s wants to be my children", sender.ToString())
	d.myPendingChildren[sender.ToString()] = sender
	pkg.Dial(sender, d.ID(), stream.NewTCPDialer())
}

func (d *DemmonTree) handlePingMessage(sender peer.Peer, m message.Message) {
	reply := Pong{Timestamp: m.(Ping).Timestamp}
	d.logger.Infof("Got ping from %s, sending pong", sender.ToString())
	pkg.SendMessageSideStream(reply, sender, d.ID(), []protocol.ID{d.ID()}, stream.NewUDPDialer())
}

func (d *DemmonTree) handlePongMessage(sender peer.Peer, m message.Message) {
	d.logger.Infof("Got pong from %s", sender.ToString())
	pongMsg := m.(Pong)
	values := d.myLatencies[sender.ToString()]

	timeTaken := time.Since(pongMsg.Timestamp)
	values.nrMeasurements++
	values.measurements = append(values.measurements, timeTaken)

	if values.nrMeasurements < d.config.NrSamplesForLatency {
		d.myLatencies[sender.ToString()] = values
		d.sendMessageTmpUDPChan(NewPingMessage(), sender)
		return
		//TODO setup timer ?
	}
	var total int64 = 0

	for _, measurement := range values.measurements {
		total += measurement.Nanoseconds() / int64(values.nrMeasurements)
	}
	values.currValue = time.Duration(total)

	d.myLatencies[sender.ToString()] = values

	for p, l := range d.myLatencies {
		d.logger.Infof("Latency to %s: %d nano (%d measurements)", p, l.currValue, l.nrMeasurements)
	}

	if d.canProgressToNextLevel() {
		d.progressToNextStep()
	}

}

func (d *DemmonTree) handleUpdateParentMessage(sender peer.Peer, m message.Message) {
	upMsg := m.(UpdateParentMessage)

	d.myLevel = upMsg.ParentLevel + 1
	d.myParent = upMsg.Parent
	d.myGrandParent = upMsg.GrandParent

	d.logger.Infof("My level : %d", d.myLevel)

	if d.myParent == nil {
		d.logger.Infof("My parent : %+v", d.myParent)
	} else {
		d.logger.Infof("My parent : %s", d.myParent.ToString())
	}
	if d.myGrandParent == nil {
		d.logger.Infof("My grandparent : %+v", d.myGrandParent)
	} else {
		d.logger.Infof("My grandparent : %s", d.myGrandParent.ToString())
	}
}
