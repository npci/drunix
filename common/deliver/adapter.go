/*
Copyright National Payments Corporation of India. All Rights Reserved.
 
SPDX-License-Identifier: Apache-2.0
*/
package deliver

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	rnd "crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type SigningIdentity struct {
	CertPath string
	KeyPath  string
	MSPID    string
}

func OrdererUserSigner() *SigningIdentity {
	// cryptoPath := consts.CRYPTO_PATH
	// files, err := ioutil.ReadDir(cryptoPath + fmt.Sprintf("/users/Admin@%s/msp/keystore/", consts.ORG))
	peerAdminKeystore := os.Getenv("CORE_PEER_ADMIN_KEYSTORE")
	if peerAdminKeystore == "" {
		logger.Panic("Environment variable CORE_PEER_ADMIN_KEYSTORE is not set or is empty. Cannot locate admin keystore directory.")
	}

	files, err := os.ReadDir(peerAdminKeystore)
	if err != nil {
		panic(fmt.Errorf("failed to read private key directory: %w", err))
	}
	keyfilePath := path.Join(peerAdminKeystore, files[0].Name())

	certPath := os.Getenv("PEER_ADMIN_SIGN_CERT")
	if certPath == "" {
		logger.Panic("Environment variable PEER_ADMIN_SIGN_CERT is not set or is empty. Cannot locate admin signing certificate.")
	}

	localMspId := viper.GetString("peer.localMspId")

	return &SigningIdentity{
		CertPath: certPath,
		KeyPath:  keyfilePath,
		MSPID:    localMspId,
	}
}

// Sign computes a SHA256 message digest, signs it with the associated private
// key, and returns the signature after low-S normlization.
func (s *SigningIdentity) Sign(msg []byte) ([]byte, error) {

	digest := sha256.Sum256(msg)

	// fmt.Printf("\ndata: %x, digest: %x\n", msg, digest)

	pemKey, err := os.ReadFile(s.KeyPath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(pemKey)
	if block.Type != "EC PRIVATE KEY" && block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("file %s does not contain a private key", s.KeyPath)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	eckey, ok := key.(*ecdsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("unexpected key type: %T", key)
	}
	r, _s, err := ecdsa.Sign(rnd.Reader, eckey, digest[:])
	if err != nil {
		return nil, err
	}
	sig, err := MarshalECDSASignature(r, _s)
	if err != nil {
		return nil, err
	}
	return SignatureToLowS(&eckey.PublicKey, sig)
}

func SignatureToLowS(k *ecdsa.PublicKey, signature []byte) ([]byte, error) {
	r, s, err := UnmarshalECDSASignature(signature)
	if err != nil {
		return nil, err
	}

	s, err = ToLowS(k, s)
	if err != nil {
		return nil, err
	}

	return MarshalECDSASignature(r, s)
}

func UnmarshalECDSASignature(raw []byte) (*big.Int, *big.Int, error) {
	sig := new(ECDSASignature)
	_, err := asn1.Unmarshal(raw, sig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed unmashalling signature [%s]", err)
	}

	// Validate sig
	if sig.R == nil {
		return nil, nil, errors.New("invalid signature, R must be different from nil")
	}
	if sig.S == nil {
		return nil, nil, errors.New("invalid signature, S must be different from nil")
	}

	if sig.R.Sign() != 1 {
		return nil, nil, errors.New("invalid signature, R must be larger than zero")
	}
	if sig.S.Sign() != 1 {
		return nil, nil, errors.New("invalid signature, S must be larger than zero")
	}

	return sig.R, sig.S, nil
}

func MarshalECDSASignature(r, s *big.Int) ([]byte, error) {
	return asn1.Marshal(ECDSASignature{r, s})
}

type ECDSASignature struct {
	R, S *big.Int
}

func ToLowS(k *ecdsa.PublicKey, s *big.Int) (*big.Int, error) {
	lowS, err := IsLowS(k, s)
	if err != nil {
		return nil, err
	}

	if !lowS {
		// Set s to N - s that will be then in the lower part of signature space
		// less or equal to half order
		s.Sub(k.Params().N, s)

		return s, nil
	}

	return s, nil
}

var curveHalfOrders = map[elliptic.Curve]*big.Int{
	elliptic.P224(): new(big.Int).Rsh(elliptic.P224().Params().N, 1),
	elliptic.P256(): new(big.Int).Rsh(elliptic.P256().Params().N, 1),
	elliptic.P384(): new(big.Int).Rsh(elliptic.P384().Params().N, 1),
	elliptic.P521(): new(big.Int).Rsh(elliptic.P521().Params().N, 1),
}

func IsLowS(k *ecdsa.PublicKey, s *big.Int) (bool, error) {
	halfOrder, ok := curveHalfOrders[k.Curve]
	if !ok {
		return false, fmt.Errorf("curve not recognized [%s]", k.Curve)
	}

	return s.Cmp(halfOrder) != 1, nil
}
