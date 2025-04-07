package container

import (
	errorutil "code-runner/error_util"
	"context"
	"fmt"
	"io"

	"github.com/docker/docker/api/types"
)

func (cs *Service) RunCommand(ctx context.Context, id string, params RunCommandParams) (io.ReadWriteCloser, string, error) {

	exec, err := cs.cli.ContainerExecCreate(ctx, id, types.ExecConfig{User: params.User, AttachStdin: true, AttachStderr: true, AttachStdout: true, Tty: true, WorkingDir: "/code-runner", Cmd: []string{"sh", "-c", params.Cmd}})
	if err != nil {
		return nil, "", errorutil.ErrorWrap(err, fmt.Sprintf("docker encountered error while executing command %q", params.Cmd))
	}
	hijackedResponse, err := cs.cli.ContainerExecAttach(ctx, exec.ID, types.ExecStartCheck{Tty: true, Detach: false})
	if err != nil {
		return nil, "", errorutil.ErrorWrap(err, fmt.Sprintf("docker encountered error while attaching to exec %q", exec))
	}
	return hijackedResponse.Conn, exec.ID, nil
}

func (cs *Service) ExecuteCommand(ctx context.Context, con io.ReadWriteCloser, stdin string) error {
	_, err := con.Write(append([]byte(stdin), '\n'))
	return err
}

func (cs *Service) CreateInteractiveShell(ctx context.Context, id string) (io.ReadWriteCloser, error) {
	exec, err := cs.cli.ContainerExecCreate(ctx, id, types.ExecConfig{
		User:         "nobody",
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		Tty:          true,
		WorkingDir:   "/code-runner",
		Cmd:          []string{"/bin/sh"},
	})
	if err != nil {
		return nil, errorutil.ErrorWrap(err, "Failed to create interactive shell")
	}

	hijackedResponse, err := cs.cli.ContainerExecAttach(ctx, exec.ID, types.ExecStartCheck{Tty: true})
	if err != nil {
		return nil, errorutil.ErrorWrap(err, "Failed to attach to interactive shell")
	}

	return hijackedResponse.Conn, nil
}
