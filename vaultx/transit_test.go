package vaultx

import (
	"context"
	"testing"
)

func TestKeyVersion(t *testing.T) {
	for ciphertext, want := range map[string]string{
		"vault:v1:abcd":   "1",
		"vault:v12:ab:cd": "12",
	} {
		if got, err := keyVersion(ciphertext); err != nil || got != want {
			t.Errorf("keyVersion(%q) = %q, %v; want %q", ciphertext, got, err, want)
		}
	}
	for _, bad := range []string{"", "v1:abcd", "vault:1:abcd", "other:v1:abcd"} {
		if _, err := keyVersion(bad); err == nil {
			t.Errorf("keyVersion(%q) accepted", bad)
		}
	}
}

func TestNewRequiresAnAddressAndOneCredential(t *testing.T) {
	for name, config := range map[string]Config{
		"no address":     {Token: "t"},
		"no credentials": {Address: "http://vault:8200"},
		"both":           {Address: "http://vault:8200", Token: "t", AppRole: &AppRole{RoleID: "r", SecretID: "s"}},
	} {
		if _, err := New(context.Background(), config); err == nil {
			t.Errorf("%s: New succeeded", name)
		}
	}
}

func TestNewCertSourceRequiresARole(t *testing.T) {
	if _, err := NewCertSource(context.Background(), &Client{}, CertSourceConfig{CommonName: "orders"}); err == nil {
		t.Error("NewCertSource without a role succeeded")
	}
}
