package job

import (
    "github.com/satori/go.uuid"
)

type Task interface {
    ID() uuid.UUID
    Run() error
    Stop()
}
