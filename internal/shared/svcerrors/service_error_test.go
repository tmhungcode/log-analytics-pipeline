package svcerrors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAsServiceError(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantErr *ServiceError
		wantOk  bool
	}{
		{
			name:    "nil input",
			err:     nil,
			wantErr: nil,
			wantOk:  false,
		},
		{
			name:    "regular error",
			err:     errors.New("x"),
			wantErr: nil,
			wantOk:  false,
		},
		{
			name:    "direct ServiceError",
			err:     NewInvalidArgumentError("LOGS_1000", "validation failed", nil),
			wantErr: NewInvalidArgumentError("LOGS_1000", "validation failed", nil),
			wantOk:  true,
		},
		{
			name:    "wrapped ServiceError",
			err:     fmt.Errorf("wrap: %w", NewInternalError("LOGS_2000", nil)),
			wantErr: NewInternalError("LOGS_2000", nil),
			wantOk:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotErr, gotOk := AsServiceError(tt.err)

			assert.Equal(t, tt.wantOk, gotOk, "AsServiceError() ok value mismatch")

			if tt.wantErr == nil {
				assert.Nil(t, gotErr, "AsServiceError() should return nil error")
			} else {
				require.NotNil(t, gotErr, "AsServiceError() should return non-nil error")
				assert.Equal(t, tt.wantErr.Category, gotErr.Category, "Category mismatch")
				assert.Equal(t, tt.wantErr.Code, gotErr.Code, "Code mismatch")
				assert.Equal(t, tt.wantErr.Message, gotErr.Message, "Message mismatch")
			}
		})
	}
}
