/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package plugin

import validation "github.com/npci/drunix/core/handlers/validation/api"

// Mapper maps plugin names to their corresponding factory instance.
// Returns nil if the name isn't associated to any plugin.
type MapperAdapter interface {
	FactoryByName(name Name) validation.PluginFactoryAdapter
}

// MapBasedMapper maps plugin names to their corresponding factories
type MapBasedMapperAdapter map[string]validation.PluginFactoryAdapter

// FactoryByName returns a plugin factory for the given plugin name, or nil if not found
func (m MapBasedMapperAdapter) FactoryByName(name Name) validation.PluginFactoryAdapter {
	return m[string(name)]
}
