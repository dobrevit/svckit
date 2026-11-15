// Package vaultx connects svckit to HashiCorp Vault for what a key or a
// certificate must not leave: data keys wrapped by Vault Transit
// (TransitWrapper, a seal.KeyWrapper) and, in a later release, short-lived
// certificates from Vault PKI.
//
// It is a module of its own, github.com/dobrevit/svckit/vaultx, so that the
// core module never depends on Vault. It talks to Vault's HTTP API with
// net/http, as secrets.VaultStore does, rather than through the official
// client: it needs a handful of endpoints, and the official client would
// bring its dependency tree into every service that imports this one.
//
// A Client authenticates with a token or with AppRole, renews its token in
// the background before it expires (and logs in again with AppRole when the
// token can no longer be renewed), and reports Vault's health for readiness.
package vaultx
