package protocol

import (
	"github.com/nm-morais/demmon-common/body_types"
	"github.com/nm-morais/go-babel/pkg/message"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/request"
)

func (d *DemmonTree) handleGetInView(req request.Request) request.Reply {
	getInViewReq := req.(GetNeighboursReq)
	view := InView{
		Grandparent: d.myGrandParent,
		Parent:      d.myParent,
		Children:    peerMapToArr(d.myChildren),
		Siblings:    peerMapToArr(d.mySiblings),
	}
	return NewGetNeighboursReqReply(getInViewReq.Key, view)
}

func (d *DemmonTree) handleBroadcastMessageReq(req request.Request) request.Reply {
	bcastReq := req.(BroadcastMessageRequest)
	wrapperMsg := NewBroadcastMessage(body_types.Message{
		ID:      bcastReq.Message.ID,
		TTL:     bcastReq.Message.TTL - 1,
		Content: bcastReq.Message.Content,
	})
	d.broadcastMessage(wrapperMsg, true, true, true)
	return nil
}

func (d *DemmonTree) broadcastMessage(msg message.Message, sendToSiblings, sendToChildren, sendToParent bool) {
	if sendToSiblings {
		for _, s := range d.mySiblings {
			d.babel.SendMessage(msg, s, d.ID(), d.ID(), true)
		}
	}

	if sendToChildren {
		for _, s := range d.myChildren {
			d.babel.SendMessage(msg, s, d.ID(), d.ID(), true)
		}
	}

	if sendToParent {
		if d.myParent != nil {
			d.babel.SendMessage(msg, d.myParent, d.ID(), d.ID(), true)
		}
	}
}

func (d *DemmonTree) handleBroadcastMessage(sender peer.Peer, m message.Message) {
	bcastMsg := m.(BroadcastMessage)
	// d.logger.Infof("got broadcastMessage %+v from %s", m, sender.String())
	d.babel.SendNotification(NewBroadcastMessageReceivedNotification(bcastMsg.Message))
	if bcastMsg.Message.TTL > 0 { // propagate
		bcastMsg.Message.TTL--
		sibling, child, parent := d.getPeerRelationshipType(sender)
		if parent || sibling {
			d.broadcastMessage(bcastMsg, false, true, false)
			return
		}

		if child {
			d.broadcastMessage(bcastMsg, true, false, true)
			return
		}
	}
}
