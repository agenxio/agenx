// +build !integration

package instance

import (
	"testing"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

func TestNewInstance(t *testing.T) {
	this, err := New("testSentry","0.9")
	if err != nil {
		panic(err)
	}

	assert.Equal(t, "testSentry", this.Info.Component)
	assert.Equal(t, "0.9", this.Info.Version)

	this, err = New("testSentry","0.9")
	if err != nil {
		panic(err)
	}
	assert.Equal(t, "testSentry", this.Info.Component)

}
