package kms

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// KeyManager defines the interface for KMS operations.
type KeyManager interface {
	// GenerateDataKey generates a new data key for the given master key ID.
	// It returns the plaintext key and an encrypted blob containing the key and metadata.
	GenerateDataKey(ctx context.Context, masterKeyID string) (plaintextKey []byte, encryptedKeyBlob []byte, err error)

	// DecryptDataKey decrypts the given encrypted key blob and returns the plaintext key.
	DecryptDataKey(ctx context.Context, encryptedKeyBlob []byte) (plaintextKey []byte, err error)
}

// LocalKMS is a local implementation of KeyManager using FoundationDB for master key storage.
type LocalKMS struct {
	db          fdb.Database
	keysDir     directory.DirectorySubspace
	cache       map[string]cachedKey
	cacheMu     sync.RWMutex
	reusePeriod time.Duration
}

type cachedKey struct {
	plaintext []byte
	encrypted []byte
	expiresAt time.Time
}

// EncryptedDataKey represents the structure stored in the message.
type EncryptedDataKey struct {
	MasterKeyID string `json:"k"`
	Ciphertext  []byte `json:"c"`
}

// NewLocalKMS creates a new LocalKMS instance.
// It requires an open FoundationDB database connection.
func NewLocalKMS(db fdb.Database) (*LocalKMS, error) {
	// Create or open the directory for KMS keys.
	// We use a separate top-level directory "kms_keys" to isolate it from application data.
	dir, err := directory.CreateOrOpen(db, []string{"kms_keys"}, nil)
	if err != nil {
		return nil, err
	}

	return &LocalKMS{
		db:          db,
		keysDir:     dir,
		cache:       make(map[string]cachedKey),
		reusePeriod: 5 * time.Minute, // Default reuse period
	}, nil
}

// GenerateDataKey generates a new data key for the given master key ID.
// It attempts to reuse a cached data key if available and valid.
func (k *LocalKMS) GenerateDataKey(ctx context.Context, masterKeyID string) ([]byte, []byte, error) {
	// 1. Check Cache
	k.cacheMu.RLock()
	if cached, ok := k.cache[masterKeyID]; ok {
		if time.Now().Before(cached.expiresAt) {
			k.cacheMu.RUnlock()
			return cached.plaintext, cached.encrypted, nil
		}
	}
	k.cacheMu.RUnlock()

	// 2. Generate new Data Key (AES-256)
	plaintextKey := make([]byte, 32)
	if _, err := rand.Read(plaintextKey); err != nil {
		return nil, nil, err
	}

	// 3. Retrieve or Create Master Key
	masterKey, err := k.getMasterKey(ctx, masterKeyID)
	if err != nil {
		return nil, nil, err
	}

	// 4. Encrypt Data Key with Master Key
	// We use the same Encrypt function that will be available in crypto.go
	encryptedKeyBytes, err := Encrypt(masterKey, plaintextKey)
	if err != nil {
		return nil, nil, err
	}

	// 5. Wrap into EncryptedDataKey struct
	edk := EncryptedDataKey{
		MasterKeyID: masterKeyID,
		Ciphertext:  encryptedKeyBytes,
	}
	edkBytes, err := json.Marshal(edk)
	if err != nil {
		return nil, nil, err
	}

	// 6. Update Cache
	k.cacheMu.Lock()
	k.cache[masterKeyID] = cachedKey{
		plaintext: plaintextKey,
		encrypted: edkBytes,
		expiresAt: time.Now().Add(k.reusePeriod),
	}
	k.cacheMu.Unlock()

	return plaintextKey, edkBytes, nil
}

// DecryptDataKey decrypts the given encrypted key blob and returns the plaintext key.
func (k *LocalKMS) DecryptDataKey(ctx context.Context, encryptedKeyBlob []byte) ([]byte, error) {
	var edk EncryptedDataKey
	if err := json.Unmarshal(encryptedKeyBlob, &edk); err != nil {
		return nil, errors.New("invalid encrypted key format")
	}

	// 1. Retrieve Master Key
	masterKey, err := k.getMasterKey(ctx, edk.MasterKeyID)
	if err != nil {
		return nil, err
	}

	// 2. Decrypt Data Key
	plaintextKey, err := Decrypt(masterKey, edk.Ciphertext)
	if err != nil {
		return nil, errors.New("failed to decrypt data key")
	}

	return plaintextKey, nil
}

// getMasterKey retrieves the master key from FDB, generating and saving it if it doesn't exist.
func (k *LocalKMS) getMasterKey(ctx context.Context, masterKeyID string) ([]byte, error) {
	keyBytes, err := k.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		keyPath := k.keysDir.Pack(tuple.Tuple{masterKeyID})
		val, err := tr.Get(keyPath).Get()
		if err != nil {
			return nil, err
		}

		if val != nil {
			return val, nil
		}

		// Generate new Master Key (AES-256)
		newKey := make([]byte, 32)
		if _, err := rand.Read(newKey); err != nil {
			return nil, err
		}

		tr.Set(keyPath, newKey)
		return newKey, nil
	})

	if err != nil {
		return nil, err
	}
	return keyBytes.([]byte), nil
}
