package docker_support

import (
	"context"
	"io"
	"time"

	"github.com/amir20/dozzle/internal/container"
	"github.com/amir20/dozzle/internal/docker"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/rs/zerolog/log"
)

type DockerJournalClientService struct {
	*DockerClientService
}

func newDockerJournalClientService(client container.Client, labels container.ContainerLabels) *DockerJournalClientService {
	return &DockerJournalClientService{
		DockerClientService: newDockerClientService(client, labels),
	}
}

func (d *DockerJournalClientService) RawLogs(
	ctx context.Context,
	container container.Container,
	from, to time.Time,
	stdTypes container.StdType) (io.ReadCloser, error) {

	reader, err := d.client.ContainerLogsBetweenDates(ctx, container.Name, from, to, stdTypes)
	if err != nil {
		return nil, err
	}

	in, out := io.Pipe()

	go func() {
		if container.Tty {
			if _, err := io.Copy(out, reader); err != nil {
				log.Error().Err(err).Msgf("error copying logs for container %s", container.ID)
			}
		} else {
			if _, err := stdcopy.StdCopy(out, out, reader); err != nil {
				log.Error().Err(err).Msgf("error copying logs for container %s", container.ID)
			}
		}

		out.Close()
	}()

	return in, nil

}

func (d *DockerJournalClientService) LogsBetweenDates(ctx context.Context, c container.Container, from time.Time, to time.Time, stdTypes container.StdType) (<-chan *container.LogEvent, error) {
	reader, err := d.client.ContainerLogsBetweenDates(ctx, c.Name, from, to, stdTypes)
	if err != nil {
		return nil, err
	}

	logReader := docker.NewJournalLogReader(reader)
	g := container.NewEventGenerator(ctx, logReader, c)
	return g.Events, nil
}

func (d *DockerJournalClientService) StreamLogs(ctx context.Context, c container.Container, from time.Time, stdTypes container.StdType, events chan<- *container.LogEvent) error {
	reader, err := d.client.ContainerLogs(ctx, c.Name, from, stdTypes)
	if err != nil {
		return err
	}

	logReader := docker.NewJournalLogReader(reader)
	g := container.NewEventGenerator(ctx, logReader, c)
	for event := range g.Events {
		events <- event
	}

	select {
	case e := <-g.Errors:
		return e
	default:
		return nil
	}
}
