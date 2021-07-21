package main

import (
	"fmt"
	"strings"

	ext2 "mosn.io/mosn/pkg/upstream/tunnel/ext"
)

func init() {
	ext2.RegisterConnectionValidator("test", &CustomValidator{})
	ext2.RegisterConnectionCredentialGetter("test", func(cluster string) string {
		return fmt.Sprintf("%s_%s", "prefix_", cluster)
	})
}

type CustomValidator struct {
}

func (c *CustomValidator) Validate(credential string, host string, cluster string) bool {
	return strings.HasPrefix(credential, "prefix_")
}
