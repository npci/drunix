/*
Copyright 2021 IBM All Rights Reserved.

SPDX-License-Identifier: Apache-2.0

Modifications Copyright National Payments Corporation of India
*/

package gateway

import (
	"io"

	gp "github.com/hyperledger/fabric-protos-go/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
DRUNIX
ConfigEvents establishes a server stream with requesting lite peers
to deliver configuration updates and chaincode approve/commit notifications.
*/
func (gs *Server) ConfigEvents(request *gp.ConfigEventsRequest, stream gp.Gateway_ConfigEventsServer) error {

	logger.Infof("Receieved config event request : %+v", request)
	if request.GetChannelId() == "" {
		return status.Error(codes.InvalidArgument, "channel id is required")
	}

	if request.GetPeerId() == "" {
		return status.Error(codes.InvalidArgument, "peer id is required")
	}

	notifyDone := make(chan struct{})
	defer close(notifyDone)

	events, err := gs.notifier.NotifyConfigEvents(notifyDone, request.ChannelId, request.PeerId)
	if err != nil {
		return toRpcError(err, codes.Aborted)
	}

	for range events {
		logger.Infof("sending config event response : %+v", request)
		if err := stream.Send(&gp.ConfigEventsResponse{ChannelId: request.ChannelId}); err != nil {
			if err == io.EOF {
				// Stream closed by the client
				return status.Error(codes.Canceled, err.Error())
			}
			return err
		}
	}
	return status.Error(codes.Internal, "events channel closed")
}
