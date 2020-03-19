package storage

import (
	"github.com/pkg/errors"
	"net/url"
	"strings"
)

func JoinPath(elem ...string) string {
	var res []string
	for _, e := range elem {
		if e != "" {
			res = append(res, strings.Trim(e, "/"))
		}
	}
	return strings.Join(res, "/")
}
func AddDelimiterToPath(path string) string {
	if strings.HasSuffix(path, "/") || path == "" {
		return path
	}
	return path + "/"
}

// TODO : unit tests
func GetPathFromPrefix(prefix string) (bucket, server string, err error) {
	bucket, server, err = ParsePrefixAsURL(prefix);
	if err != nil {
		return "", "", err
	}

	// Allover the code this parameter is concatenated with '/'.
	// TODO: Get rid of numerous string literals concatenated with this
	server = strings.Trim(server, "/")

	return bucket, server, nil
}

// TODO : unit tests
func ParsePrefixAsURL(prefix string) (bucket, server string, err error) {
	storageUrl, err := url.Parse(prefix)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to parse url '%s'", prefix)
	}
	if storageUrl.Scheme == "" || storageUrl.Host == "" {
		return "", "", errors.Errorf("missing url scheme=%q and/or host=%q", storageUrl.Scheme, storageUrl.Host)
	}

	return storageUrl.Host, storageUrl.Path, nil
}
