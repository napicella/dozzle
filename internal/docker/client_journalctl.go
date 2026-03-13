package docker

import (
	"context"
	"io"
	"os/exec"
	"time"

	"github.com/amir20/dozzle/internal/container"
	"github.com/docker/docker/client"
	"github.com/rs/zerolog/log"
)

type DockerJournalClient struct {
	*DockerClient
	host container.Host
}

func NewDockerJournalClient(hostname string) (*DockerJournalClient, error) {
	log.Info().Msg("Using journalctl to retrieve logs")

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation(), client.WithUserAgent("Docker-Client/Dozzle"))

	if err != nil {
		return nil, err
	}

	info, err := cli.Info(context.Background())
	if err != nil {
		return nil, err
	}

	host := container.Host{
		Name:     info.Name,
		Endpoint: "local",
		Type:     "local",
	}

	if hostname != "" {
		host.Name = hostname
	}

	decoree, err := NewLocalClient(hostname)
	if err != nil {
		return nil, err
	}

	return &DockerJournalClient{
		DockerClient: decoree,
		host:         host,
	}, nil
}

// ContainerLogs streams live logs for the named container from journalctl.
// `id` is treated as the container name (matched via CONTAINER_NAME= journal field).
// The stream follows new entries until the context is cancelled.
func (t *DockerJournalClient) ContainerLogs(
	ctx context.Context, id string,
	since time.Time,
	stdType container.StdType) (io.ReadCloser, error) {

	log.Info().Msg("Retrieving container logs with journalctl")
	args := []string{
		"--follow",
		"--output", "json",
		"--no-pager",
		"CONTAINER_NAME=" + id,
	}

	since = since.Add(-5 * time.Hour)
	if !since.IsZero() {
		args = append(args, "--since", since.Format("2006-01-02 15:04:05"))
	}

	return startJournalctl(ctx, args)
}

// ContainerLogsBetweenDates fetches historical logs for the named container
// between `from` and `to` from journalctl (no follow).
// `id` is treated as the container name (matched via CONTAINER_NAME= journal field).
func (d *DockerJournalClient) ContainerLogsBetweenDates(
	ctx context.Context,
	id string,
	from, to time.Time,
	stdType container.StdType) (io.ReadCloser, error) {

	args := []string{
		"--output", "json",
		"--no-pager",
		"CONTAINER_NAME=" + id,
	}
	if !from.IsZero() {
		from = from.Add(-5 * time.Hour)
		args = append(args, "--since", from.Format("2006-01-02 15:04:05"))
	}
	if !to.IsZero() {
		args = append(args, "--until", to.Format("2006-01-02 15:04:05"))
	}

	return startJournalctl(ctx, args)
}

// startJournalctl starts a journalctl process with the given arguments and
// returns a pipe connected to its combined stdout+stderr.
// The pipe is closed automatically when the process exits.
func startJournalctl(ctx context.Context, args []string) (io.ReadCloser, error) {
	cmd := exec.CommandContext(ctx, "journalctl", args...)
	log.Info().Msgf("Running %s\n", cmd.String())

	pr, pw := io.Pipe()
	cmd.Stdout = pw
	cmd.Stderr = pw

	if err := cmd.Start(); err != nil {
		pw.Close()
		return nil, err
	}

	go func() {
		cmd.Wait()
		pw.Close()
	}()

	return pr, nil
}
