package swift

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/storages/storage"
	"os"
	"testing"
)

var settings = map[string]string{
	"OS_USERNAME":    "",
	"OS_PASSWORD":    "",
	"OS_AUTH_URL":    "",
	"OS_TENANT_NAME": "",
	"OS_REGION_NAME": "",
}

func TestSwiftFolderUsingConfigFile(t *testing.T) {
	t.Skip("Credentials needed to run Swift Storage tests")

	storageFolderUsingConfigFile, err := ConfigureFolder("swift://test-container/test-folder/sub0", settings)
	assert.NoError(t, err)
	storage.RunFolderTest(storageFolderUsingConfigFile, t)
}

func TestSwiftFolderUsingEnvVariables(t *testing.T) {
	t.Skip("Credentials needed to run Swift Storage tests")

	for prop, value := range settings {
		os.Setenv(prop, value)
	}

	storageFolderUsingEnvVars, err := ConfigureFolder("swift://test-container/test-folder/sub0", nil)
	assert.NoError(t, err)
	storage.RunFolderTest(storageFolderUsingEnvVars, t)
}
