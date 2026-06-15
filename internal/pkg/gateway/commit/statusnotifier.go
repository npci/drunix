/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Modifications Copyright National Payments Corporation of India
*/

package commit

import (
	"sync"

	"github.com/npci/drunix/core/ledger"

	cp "github.com/hyperledger/fabric-protos-go/common"
)

type statusListenerSet map[*statusListener]struct{}

type statusNotifier struct {
	lock              sync.Mutex
	configLock        sync.Mutex
	listenersByTxID   map[string]statusListenerSet
	listenersByPeerID map[string]statusListenerSet
	closed            bool
}

func newStatusNotifier() *statusNotifier {
	return &statusNotifier{
		listenersByTxID:   make(map[string]statusListenerSet),
		listenersByPeerID: make(map[string]statusListenerSet),
	}
}

/*
DRUNIX
When block events are received and the TxsInfo list is empty, the block can be treated as a config block.
If transaction chaincode names include _lifecycle, the block can be marked as an approve/commit for chaincode,
and an event is emitted to all registered lite peers.
*/
func (notifier *statusNotifier) ReceiveBlock(blockEvent *ledger.CommitNotification) {
	notifier.removeCompletedListeners()
	notifier.removeCompletedConfigListeners()

	if len(blockEvent.TxsInfo) == 0 {
		statusEvent := &Status{
			BlockNumber: blockEvent.BlockNumber,
		}
		notifier.notifyConfig(statusEvent)
	}

	for _, txInfo := range blockEvent.TxsInfo {
		statusEvent := &Status{
			BlockNumber:   blockEvent.BlockNumber,
			TransactionID: txInfo.TxID,
			Code:          txInfo.ValidationCode,
		}
		notifier.notify(statusEvent)

		if txInfo.TxType == cp.HeaderType_CONFIG || (txInfo.ChaincodeID != nil && txInfo.ChaincodeID.Name == "_lifecycle") {
			notifier.notifyConfig(statusEvent)
		}
	}
}

func (notifier *statusNotifier) removeCompletedListeners() {
	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	for transactionID, listeners := range notifier.listenersByTxID {
		for listener := range listeners {
			if listener.isDone() {
				notifier.removeListener(transactionID, listener)
			}
		}
	}
}

func (notifier *statusNotifier) notify(event *Status) {
	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	for listener := range notifier.listenersByTxID[event.TransactionID] {
		listener.receive(event)
		notifier.removeListener(event.TransactionID, listener)
	}
}

func (notifier *statusNotifier) registerListener(done <-chan struct{}, transactionID string) <-chan *Status {
	notifyChannel := make(chan *Status, 1) // Avoid blocking and only expect one notification per channel

	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	if notifier.closed {
		close(notifyChannel)
	} else {
		listener := &statusListener{
			done:          done,
			notifyChannel: notifyChannel,
		}
		notifier.listenersForTxID(transactionID)[listener] = struct{}{}
	}

	return notifyChannel
}

func (notifier *statusNotifier) listenersForTxID(transactionID string) statusListenerSet {
	listeners, exists := notifier.listenersByTxID[transactionID]
	if !exists {
		listeners = make(statusListenerSet)
		notifier.listenersByTxID[transactionID] = listeners
	}

	return listeners
}

func (notifier *statusNotifier) removeListener(transactionID string, listener *statusListener) {
	listener.close()

	listeners := notifier.listenersByTxID[transactionID]
	delete(listeners, listener)

	if len(listeners) == 0 {
		delete(notifier.listenersByTxID, transactionID)
	}
}

func (notifier *statusNotifier) Close() {
	notifier.lock.Lock()
	defer notifier.lock.Unlock()

	for _, listeners := range notifier.listenersByTxID {
		for listener := range listeners {
			listener.close()
		}
	}

	notifier.listenersByTxID = nil

	notifier.configLock.Lock()
	defer notifier.configLock.Unlock()
	for _, listeners := range notifier.listenersByPeerID {
		for listener := range listeners {
			listener.close()
		}
	}
	notifier.listenersByPeerID = nil

	notifier.closed = true
}

type statusListener struct {
	done          <-chan struct{}
	notifyChannel chan<- *Status
}

func (listener *statusListener) isDone() bool {
	select {
	case <-listener.done:
		return true
	default:
		return false
	}
}

func (listener *statusListener) close() {
	close(listener.notifyChannel)
}

func (listener *statusListener) receive(event *Status) {
	listener.notifyChannel <- event
}
