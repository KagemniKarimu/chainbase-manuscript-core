package pkg

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
)

type PortReservation struct {
	Port           int
	ManuscriptName string
	PortType       string // "Flink", "GraphQL", "DB"
}

func FindReservedPorts(config *Config) (map[int]PortReservation, error) {
	reserved := make(map[int]PortReservation)

	if config == nil {
		return reserved, nil
	}

	for _, ms := range config.Manuscripts {
		// Check Flink port
		if ms.Port != 0 {
			if existing, exists := reserved[ms.Port]; exists {
				return nil, fmt.Errorf("port conflict detected: %d is reserved by both %s and %s for %s",
					ms.Port, existing.ManuscriptName, ms.Name, existing.PortType)
			}
			reserved[ms.Port] = PortReservation{
				Port:           ms.Port,
				ManuscriptName: ms.Name,
				PortType:       "Flink",
			}
		}

		// Check GraphQL port
		if ms.GraphQLPort != 0 {
			if existing, exists := reserved[ms.GraphQLPort]; exists {
				return nil, fmt.Errorf("port conflict detected: %d is reserved by both %s and %s for %s",
					ms.GraphQLPort, existing.ManuscriptName, ms.Name, existing.PortType)
			}
			reserved[ms.GraphQLPort] = PortReservation{
				Port:           ms.GraphQLPort,
				ManuscriptName: ms.Name,
				PortType:       "GraphQL",
			}
		}

		// Check DB port
		if ms.DbPort != 0 {
			if existing, exists := reserved[ms.DbPort]; exists {
				return nil, fmt.Errorf("port conflict detected: %d is reserved by both %s and %s for %s",
					ms.DbPort, existing.ManuscriptName, ms.Name, existing.PortType)
			}
			reserved[ms.DbPort] = PortReservation{
				Port:           ms.DbPort,
				ManuscriptName: ms.Name,
				PortType:       "DB",
			}
		}
	}

	return reserved, nil
}

func ValidatePortAssignments(ms *Manuscript, config *Config) error {
	reserved, err := FindReservedPorts(config)
	if err != nil {
		return fmt.Errorf("failed to check reserved ports: %w", err)
	}

	// Validate manuscript ports against reservations
	if ms.Port != 0 {
		if reservation, exists := reserved[ms.Port]; exists && reservation.ManuscriptName != ms.Name {
			return fmt.Errorf("port %d is already reserved by manuscript %s for %s",
				ms.Port, reservation.ManuscriptName, reservation.PortType)
		}
	}

	if ms.GraphQLPort != 0 {
		if reservation, exists := reserved[ms.GraphQLPort]; exists && reservation.ManuscriptName != ms.Name {
			return fmt.Errorf("port %d is already reserved by manuscript %s for %s",
				ms.GraphQLPort, reservation.ManuscriptName, reservation.PortType)
		}
	}

	if ms.DbPort != 0 {
		if reservation, exists := reserved[ms.DbPort]; exists && reservation.ManuscriptName != ms.Name {
			return fmt.Errorf("port %d is already reserved by manuscript %s for %s",
				ms.DbPort, reservation.ManuscriptName, reservation.PortType)
		}
	}

	return nil
}

func InitializePorts(ms *Manuscript, config *Config) error {
	// First validate any existing port assignments
	if err := ValidatePortAssignments(ms, config); err != nil {
		return err
	}

	// Get both active and reserved ports
	listeningPorts, err := GetListeningPorts()
	if err != nil {
		return fmt.Errorf("failed to get listening ports: %w", err)
	}

	reservedPorts, err := FindReservedPorts(config)
	if err != nil {
		return fmt.Errorf("failed to get reserved ports: %w", err)
	}

	// Combine active and reserved ports
	unavailablePorts := make(map[int]bool)
	for _, port := range listeningPorts {
		unavailablePorts[port] = true
	}
	for port := range reservedPorts {
		unavailablePorts[port] = true
	}

	// Initialize Flink port if not set
	if ms.Port == 0 {
		port, err := FindAvailablePort(8081, 8181, unavailablePorts)
		if err != nil {
			return fmt.Errorf("failed to find available port for Flink: %w", err)
		}
		ms.Port = port
		unavailablePorts[port] = true
	}

	// Initialize GraphQL port if not set
	if ms.GraphQLPort == 0 {
		port, err := FindAvailablePort(8082, 8182, unavailablePorts)
		if err != nil {
			return fmt.Errorf("failed to find available port for GraphQL: %w", err)
		}
		ms.GraphQLPort = port
		unavailablePorts[port] = true
	}

	// Initialize DB port if not set
	if ms.DbPort == 0 {
		port, err := FindAvailablePort(15432, 15532, unavailablePorts)
		if err != nil {
			return fmt.Errorf("failed to find available port for DB: %w", err)
		}
		ms.DbPort = port
	}

	return nil
}

func GetListeningPorts() ([]int, error) {
	ports := make(map[int]bool)

	// Check system ports using lsof
	cmd := exec.Command("lsof", "-nP", "-iTCP", "-sTCP:LISTEN")
	var out bytes.Buffer
	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		// Don't return error here, continue to check Docker ports
		fmt.Printf("Warning: Unable to check system ports: %v\n", err)
	} else {
		re := regexp.MustCompile(`:(\d+)\s+\(LISTEN\)`)
		scanner := bufio.NewScanner(&out)
		for scanner.Scan() {
			line := scanner.Text()
			matches := re.FindStringSubmatch(line)
			if len(matches) > 1 {
				port, err := strconv.Atoi(matches[1])
				if err != nil {
					continue
				}
				ports[port] = true
			}
		}
	}

	// Check Docker container ports
	dockerCmd := exec.Command("docker", "ps", "--format", "{{.Ports}}")
	var dockerOut bytes.Buffer
	dockerCmd.Stdout = &dockerOut

	if err := dockerCmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to check Docker ports: %w", err)
	}

	// Parse Docker port mappings
	scanner := bufio.NewScanner(&dockerOut)
	portRegex := regexp.MustCompile(`0\.0\.0\.0:(\d+)`)
	for scanner.Scan() {
		line := scanner.Text()
		matches := portRegex.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) > 1 {
				port, err := strconv.Atoi(match[1])
				if err != nil {
					continue
				}
				ports[port] = true
			}
		}
	}

	// Convert map to slice
	var result []int
	for port := range ports {
		result = append(result, port)
	}
	return result, nil
}

func FindAvailablePort(startPort, endPort int, unavailablePorts map[int]bool) (int, error) {
	for port := startPort; port <= endPort; port++ {
		if !unavailablePorts[port] {
			return port, nil
		}
	}
	return 0, fmt.Errorf("no available ports in the range %d-%d", startPort, endPort)
}
