/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package kafka

import (
	"reflect"
	"testing"

	"github.com/npci/drunix/internal/pkg/gateway/event/publisher"
)

func TestNewKafkaPublisher(t *testing.T) {
	type args struct {
		fetchData func() ([]string, *KafkaPublisher)
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{

		{
			name: "failed to create publisher as broker was not available",
			args: args{
				fetchData: func() ([]string, *KafkaPublisher) {
					urls := make([]string, 0)
					urls = append(urls, "localhost:9090")
					return urls, nil
				},
			},
			wantErr: true,
		},

		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, want := tt.args.fetchData()
			got, err := NewKafkaPublisher(publisher.Config{})
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKafkaPublisher() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("NewKafkaPublisher() = %v, want %v", got, want)
			}
		})
	}
}
