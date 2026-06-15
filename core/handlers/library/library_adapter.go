/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
Modifications Copyright National Payments Corporation of India
*/
package library

import (
	validation "github.com/npci/drunix/core/handlers/validation/api"
	vb "github.com/npci/drunix/core/handlers/validation/builtin"
)

func (r *HandlerLibrary) DefaultValidationAdapter() validation.PluginFactoryAdapter {
	return &vb.DefaultValidationFactoryAdapter{}
}
