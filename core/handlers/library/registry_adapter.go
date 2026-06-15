/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package library

import (
	"reflect"
	"sync"

	"github.com/npci/drunix/core/handlers/auth"
	"github.com/npci/drunix/core/handlers/decoration"
	endorsement2 "github.com/npci/drunix/core/handlers/endorsement/api"
	validation "github.com/npci/drunix/core/handlers/validation/api"
)

type registryAdapter struct {
	*registry
	validators map[string]validation.PluginFactoryAdapter
}

var (
	onceAdapter sync.Once
	regAdapter  registryAdapter
)

// InitRegistry creates the (only) instance
// of the registry
func InitRegistryAdapter(c Config) Registry {
	onceAdapter.Do(func() {
		regAdapter = registryAdapter{
			registry: &registry{
				endorsers:  make(map[string]endorsement2.PluginFactory),
				validators: make(map[string]validation.PluginFactory),
			},
			validators: make(map[string]validation.PluginFactoryAdapter),
		}
		regAdapter.loadHandlers(c)
	})
	return &regAdapter
}

// Lookup returns a list of handlers with the given
// type, or nil if none exist
func (r *registryAdapter) Lookup(handlerType HandlerType) interface{} {
	if handlerType == Auth {
		return r.filters
	} else if handlerType == Decoration {
		return r.decorators
	} else if handlerType == Endorsement {
		return r.endorsers
	} else if handlerType == Validation {
		return r.validators
	}

	return nil
}

// loadHandlers loads the configured handlers
func (r *registryAdapter) loadHandlers(c Config) {
	for _, config := range c.AuthFilters {
		r.evaluateModeAndLoad(config, Auth)
	}
	for _, config := range c.Decorators {
		r.evaluateModeAndLoad(config, Decoration)
	}

	for chaincodeID, config := range c.Endorsers {
		r.evaluateModeAndLoad(config, Endorsement, chaincodeID)
	}

	for chaincodeID, config := range c.Validators {
		r.evaluateModeAndLoad(config, Validation, chaincodeID)
	}
}

// evaluateModeAndLoad if a library path is provided, load the shared object
func (r *registryAdapter) evaluateModeAndLoad(c *HandlerConfig, handlerType HandlerType, extraArgs ...string) {
	if c.Library != "" {
		r.loadPlugin(c.Library, handlerType, extraArgs...)
	} else {
		r.loadCompiled(c.Name, handlerType, extraArgs...)
	}
}

func (r *registryAdapter) loadCompiled(handlerFactory string, handlerType HandlerType, extraArgs ...string) {

	registryMD := reflect.ValueOf(&HandlerLibrary{})

	o := registryMD.MethodByName(handlerFactory)
	if !o.IsValid() {
		logger.Panicf("Method %s isn't a method of HandlerLibrary", handlerFactory)
	}

	inst := o.Call(nil)[0].Interface()

	if handlerType == Auth {
		r.filters = append(r.filters, inst.(auth.Filter))
	} else if handlerType == Decoration {
		r.decorators = append(r.decorators, inst.(decoration.Decorator))
	} else if handlerType == Endorsement {
		if len(extraArgs) != 1 {
			logger.Panicf("expected 1 argument in extraArgs")
		}
		r.endorsers[extraArgs[0]] = inst.(endorsement2.PluginFactory)
	} else if handlerType == Validation {
		if len(extraArgs) != 1 {
			logger.Panicf("expected 1 argument in extraArgs")
		}
		r.validators[extraArgs[0]] = inst.(validation.PluginFactoryAdapter)
	}
}
