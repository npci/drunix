/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
/*
	DRUNIX
*/
package sparseblock

type CacheStore interface {
	GetLocalStore()
}

type OrgChainWriter interface {
	Put([]byte, []byte)
}

type OrgChainReader interface {
	Get([]byte) ([]byte, bool)
}

type OrgChainReadWriter interface {
	OrgChainWriter
	OrgChainReader
}
