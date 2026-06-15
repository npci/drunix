/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/

package commit

import (
	"fmt"
	"strings"

	"github.com/npci/drunix/common/flogging"
)

var logger = flogging.MustGetLogger("gateway")

/*
	DRUNIX
	The config notifier informs all registered lite peers on the committing peer,
	allowing them to reinitialize upon configuration commits.
*/

// notifyConfigEvents notifies the caller when config transaction commits on the named channel. The caller is only notified
// of commits occurring after registering for notifications.
func (n *Notifier) NotifyConfigEvents(done <-chan struct{}, channelName string, peerID string) (<-chan *Status, error) {
	notifiers, err := n.notifiersForChannel(channelName)
	if err != nil {
		return nil, err
	}

	notifyChannel := notifiers.status.registerConfigListener(done, peerID)
	return notifyChannel, nil
}

func (notifier *statusNotifier) registerConfigListener(done <-chan struct{}, peerID string) <-chan *Status {
	notifyChannel := make(chan *Status)

	notifier.configLock.Lock()
	defer notifier.configLock.Unlock()

	if notifier.closed {
		close(notifyChannel)
	} else {
		listener := &statusListener{
			done:          done,
			notifyChannel: notifyChannel,
		}
		notifier.listenersForConfigTxn(peerID)[listener] = struct{}{}
	}

	return notifyChannel
}

func (notifier *statusNotifier) listenersForConfigTxn(peerID string) statusListenerSet {
	listeners, exists := notifier.listenersByPeerID[peerID]
	if !exists {
		listeners = make(statusListenerSet)
		notifier.listenersByPeerID[peerID] = listeners
	}

	return listeners
}

func (notifier *statusNotifier) notifyConfig(event *Status) {
	listenerNames := []string{}
	for peerID, listeners := range notifier.listenersByPeerID {
		listenerNames = append(listenerNames, fmt.Sprintf("[%s:%d]", peerID, len(listeners)))
		for listener := range listeners {
			listener.receive(event)
		}
	}
	logger.Infof("Config Listeners : [%s]\n", strings.Join(listenerNames, ","))
}

func (notifier *statusNotifier) removeCompletedConfigListeners() {
	notifier.configLock.Lock()
	defer notifier.configLock.Unlock()

	for peerID, listeners := range notifier.listenersByPeerID {
		for listener := range listeners {
			if listener.isDone() {
				notifier.removeConfigListener(peerID, listener)
			}
		}
	}
}

func (notifier *statusNotifier) removeConfigListener(peerID string, listener *statusListener) {
	listener.close()

	listeners := notifier.listenersByPeerID[peerID]
	delete(listeners, listener)

	if len(listeners) == 0 {
		delete(notifier.listenersByPeerID, peerID)
	}
}
