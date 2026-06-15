/*
Copyright National Payments Corporation of India. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package event

import (
	"github.com/npci/drunix/internal/pkg/gateway/event/kafka"

	"github.com/npci/drunix/internal/pkg/gateway/event/publisher"

	"github.com/pkg/errors"
)

/*
NewPublisher:
Type: Function

NewPublisher creates a new publisher depending on the server type.
Parameters Required:
  - config: config used to create the publisher

Return Parameters:
  - Publisher: Publisher implementation
  - error: error if any
*/
func NewPublisher(config publisher.Config) (publisher.Publisher, error) {
	switch config.ServerType {

	case publisher.KAFKA:
		return kafka.NewKafkaPublisher(config)
	case publisher.GRPC:
		return nil, errors.Errorf("event publisher server type `GRPC` is not implemented yet")
	case publisher.UNIMPLEMENTED:
		return publisher.NewUnimplementedPublisher(), nil
	default:
		return nil, errors.Errorf("event publisher server type `%v` is not supported", config.ServerType)
	}
}
