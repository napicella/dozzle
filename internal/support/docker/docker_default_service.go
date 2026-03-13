package docker_support

import "github.com/amir20/dozzle/internal/container"

// NewDockerClientService creates the default docker client service.
func NewDockerClientService(client container.Client, labels container.ContainerLabels) *DockerJournalClientService {
	return newDockerJournalClientService(client, labels)
}
