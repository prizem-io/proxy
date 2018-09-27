// Copyright 2018 The Prizem Authors. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package tls

import (
	"crypto/tls"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"unsafe"

	"github.com/prizem-io/proxy/pkg/log"
)

type KeyPairReloader struct {
	logger   log.Logger
	cert     unsafe.Pointer // Stores a *tls.Certificate
	certPath string
	keyPath  string

	reload chan os.Signal
}

func NewKeyPairReloader(logger log.Logger, certPath, keyPath string) (*KeyPairReloader, error) {
	reloader := KeyPairReloader{
		logger:   logger,
		certPath: certPath,
		keyPath:  keyPath,
		reload:   make(chan os.Signal, 1),
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	reloader.cert = unsafe.Pointer(&cert)
	go func() {
		signal.Notify(reloader.reload, syscall.SIGHUP)
		for range reloader.reload {
			reloader.maybeReload()
		}
	}()
	return &reloader, nil
}

func (k *KeyPairReloader) Reload() {
	k.reload <- syscall.SIGHUP
}

func (k *KeyPairReloader) maybeReload() error {
	k.logger.Infof("Attempting to reload TLS certificate and key from %q and %q", k.certPath, k.keyPath)
	newCert, err := tls.LoadX509KeyPair(k.certPath, k.keyPath)
	if err != nil {
		k.logger.Infof("Keeping old TLS certificate because the new one could not be loaded: %v", err)
		return err
	}
	atomic.StorePointer(&k.cert, unsafe.Pointer(&newCert))
	return nil
}

func (k *KeyPairReloader) GetCertificateFunc() func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		cert := (*tls.Certificate)(atomic.LoadPointer(&k.cert))
		return cert, nil
	}
}
