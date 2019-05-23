package openpgp

import (
	"bufio"
	"bytes"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"golang.org/x/crypto/openpgp"
)

// Crypter incapsulates specific of cypher method
// Includes keys, infrastructure information etc
// If many encryption methods will be used it worth
// to extract interface
type Crypter struct {
	KeyRingID      string
	IsUseKeyRingID bool

	ArmoredKey      string
	IsUseArmoredKey bool

	ArmoredKeyPath      string
	IsUseArmoredKeyPath bool

	PubKey    openpgp.EntityList
	SecretKey openpgp.EntityList

	mutex sync.RWMutex
}

type CrypterInitializationError struct {
	error
}

// NewCrypterInitializationError creates new instance of CrypterInitializationError
func NewCrypterInitializationError(message string) CrypterInitializationError {
	return CrypterInitializationError{errors.New(message)}
}

func initCrypter(crypter *Crypter, passphrase string) (*Crypter, error) {
	if !crypter.isArmed() {
		return nil, NewCrypterInitializationError("crypter is not armed")
	}
	err := crypter.loadSecret(passphrase)
	if err != nil {
		return nil, NewCrypterInitializationError("failed to load secret")
	}
	return crypter, nil
}

// CrypterFromKey creates Crypter from armored key.
func CrypterFromKey(armoredKey, passphrase string) (crypto.Crypter, error) {
	crypter := &Crypter{ArmoredKey: armoredKey, IsUseArmoredKey: true}
	return initCrypter(crypter, passphrase)
}

// CrypterFromKeyPath creates Crypter from armored key path.
func CrypterFromKeyPath(armoredKeyPath, passphrase string) (crypto.Crypter, error) {
	crypter := &Crypter{ArmoredKeyPath: armoredKeyPath, IsUseArmoredKeyPath: true}
	return initCrypter(crypter, passphrase)
}

// CrypterFromKeyRingID create Crypter from key ring ID.
func CrypterFromKeyRingID(keyRingID, passphrase string) (crypto.Crypter, error) {
	crypter := &Crypter{KeyRingID: keyRingID, IsUseKeyRingID: true}
	return initCrypter(crypter, passphrase)
}

// CrypterFromKeyRing creates Crypter from armored keyring.
// It is used mainly for mock purpouses, so it panics on error.
func CrypterFromKeyRing(armedKeyring string) crypto.Crypter {
	ring, err := openpgp.ReadArmoredKeyRing(strings.NewReader(armedKeyring))
	if err != nil {
		panic(err)
	}
	crypter := &Crypter{PubKey: ring, SecretKey: ring}
	return crypter
}

func (crypter *Crypter) isArmed() bool {
	if crypter.IsUseKeyRingID {
		tracelog.WarningLogger.Println(`
You are using deprecated functionality that uses an external gpg library.
It will be removed in next major version.
Please set GPG key using environment variables WALG_PGP_KEY or WALG_PGP_KEY_PATH.
		`)
	}

	return crypter.IsUseArmoredKey || crypter.IsUseArmoredKeyPath || crypter.IsUseKeyRingID
}

func (crypter *Crypter) setupPubKey() error {
	crypter.mutex.RLock()
	if crypter.PubKey != nil {
		crypter.mutex.RUnlock()
		return nil
	}
	crypter.mutex.RUnlock()

	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()
	if crypter.PubKey != nil { // already set up
		return nil
	}

	if crypter.IsUseArmoredKey {
		entityList, err := openpgp.ReadArmoredKeyRing(strings.NewReader(crypter.ArmoredKey))

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	} else if crypter.IsUseArmoredKeyPath {
		entityList, err := readPGPKey(crypter.ArmoredKeyPath)

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	} else {
		// TODO: legacy gpg external use, need to remove in next major version
		armor, err := crypto.GetPubRingArmor(crypter.KeyRingID)

		if err != nil {
			return err
		}

		entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

		if err != nil {
			return err
		}

		crypter.PubKey = entityList
	}
	return nil
}

// Encrypt creates encryption writer from ordinary writer
func (crypter *Crypter) Encrypt(writer io.Writer) (io.WriteCloser, error) {
	err := crypter.setupPubKey()
	if err != nil {
		return nil, err
	}

	// We use buffered writer because encryption starts writing header immediately,
	// which can be inappropriate for further usage with blocking writers.
	// E. g. if underlying writer is a pipe, then this thread will be blocked before
	// creation of new thread, reading from this pipe.Writer.
	bufferedWriter := bufio.NewWriter(writer)
	encryptedWriter, err := openpgp.Encrypt(bufferedWriter, crypter.PubKey, nil, nil, nil)

	if err != nil {
		return nil, errors.Wrapf(err, "opengpg encryption error")
	}

	return ioextensions.NewOnCloseFlusher(encryptedWriter, bufferedWriter), nil
}

// Decrypt creates decrypted reader from ordinary reader
func (crypter *Crypter) Decrypt(reader io.Reader) (io.Reader, error) {
	md, err := openpgp.ReadMessage(reader, crypter.SecretKey, nil, nil)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	return md.UnverifiedBody, nil
}

// load the secret key based on the settings
func (crypter *Crypter) loadSecret(passphrase string) error {
	// check if we actually need to load it
	crypter.mutex.RLock()
	if crypter.SecretKey != nil {
		crypter.mutex.RUnlock()
		return nil
	}
	// unlock needs to be there twice due to different code paths
	crypter.mutex.RUnlock()

	// we need to load, so lock for writing
	crypter.mutex.Lock()
	defer crypter.mutex.Unlock()

	// double check as the key might have been loaded between the RUnlock and Lock
	if crypter.SecretKey != nil {
		return nil
	}

	if crypter.IsUseArmoredKey {
		entityList, err := openpgp.ReadArmoredKeyRing(strings.NewReader(crypter.ArmoredKey))

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	} else if crypter.IsUseArmoredKeyPath {
		entityList, err := readPGPKey(crypter.ArmoredKeyPath)

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	} else {
		// TODO: legacy gpg external use, need to remove in next major version
		armor, err := crypto.GetSecretRingArmor(crypter.KeyRingID)

		if err != nil {
			return errors.WithStack(err)
		}

		entityList, err := openpgp.ReadArmoredKeyRing(bytes.NewReader(armor))

		if err != nil {
			return errors.WithStack(err)
		}

		crypter.SecretKey = entityList
	}

	err := decryptSecretKey(crypter.SecretKey, passphrase)

	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
