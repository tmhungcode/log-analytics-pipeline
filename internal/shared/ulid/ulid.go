package ulid

import (
	"github.com/oklog/ulid/v2"
)

// NewULID generates a new ULID string.
var NewULID = func() string {
	return ulid.Make().String()
}
