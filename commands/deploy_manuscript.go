package commands

import (
	"fmt"
	"log"
	"manuscript-core/pkg"
	"os"
	"path/filepath"
	"strings"
)

func DeployManuscript(args []string) {
	if len(args) < 1 {
		log.Fatalf("Error: Manuscript path is required as the first argument.")
	}
	manuscriptPath := args[0]
	var ms pkg.Manuscript
	var manuscriptDir string

	if _, err := os.Stat(manuscriptPath); os.IsNotExist(err) {
		log.Fatalf("Error: Manuscript file does not exist: %s", manuscriptPath)
	}

	steps := []struct {
		name string
		fn   func() error
	}{
		{"Step 1: Validating manuscript file", func() error {
			return validateManuscriptFile(manuscriptPath)
		}},
		{"Step 2: Parsing manuscript yaml", func() error {
			var err error
			ms, err = ParseManuscriptYaml(manuscriptPath)
			if err != nil {
				return err
			}
			if len(ms.Sinks) != 0 {
				ms.Table = ms.Sinks[0].Table
				ms.Database = ms.Sinks[0].Database
			}
			if len(ms.Sources) != 0 {
				ms.Chain = ms.Sources[0].Dataset
			}
			if len(ms.Transforms) != 0 {
				ms.Query = ms.Transforms[0].SQL
			}

			manuscriptDir = getHomeDir()
			msConfig, err := pkg.LoadConfig(manuscriptConfig)
			if err != nil {
				logErrorAndReturn("Failed to load manuscript config", err)
			}
			if msConfig.BaseDir != "" {
				manuscriptDir = msConfig.BaseDir
			}

			if strings.HasSuffix(manuscriptDir, "/") {
				manuscriptDir = strings.TrimSuffix(manuscriptDir, "/")
			}
			manuscriptDir = fmt.Sprintf("%s/%s/%s", manuscriptDir, manuscriptBaseName, ms.Name)

			if len(ms.Sinks) != 0 {
				if ms.Sinks[0].Type == "postgres" {
					ms.Sink = "postgres"
				}
			}
			return nil
		}},
		{"Step 3: Verifying Port Initialization", func() error { return pkg.InitializePorts(&ms) }},
		{"Step 4: Checking manuscript is already deployed", func() error {
			err := CheckManuscriptExist(ms)
			if err != nil {
				return err
			}
			return nil
		}},
		{"Step 5: Create Directory", func() error { return createDirectory(manuscriptDir) }},
		{"Step 6: Create ManuscriptFile", func() error { return copyManuscriptFile(manuscriptDir, manuscriptPath) }},
		{"Step 7: Create DockerComposeFile", func() error { return createDockerComposeFile(manuscriptDir, &ms) }},
		{"Step 8: Check Docker Installed", func() error { return checkDockerInstalled() }},
		{"Step 9: Start Docker Containers", func() error { return startDockerContainers(manuscriptDir) }},
		{"Step 10: Check Container Status", func() error { return checkContainerStatus(&ms) }},
	}

	for _, step := range steps {
		err := pkg.ExecuteStepWithLoading(step.name, true, step.fn)
		if err != nil {
			log.Fatalf("\033[31m✗ %s failed: %v\n", step.name, err)
		}
	}
	fmt.Println("\033[32m✓ Manuscript deployment completed successfully!")
	log.Printf("\033[32mYou can now list your job with the command: \n👉 \033[33mmanuscript-cli list\n\n"+
		"\033[32mIf you need to manually edit the manuscript, "+
		"you can edit the file '%s/manuscript.yaml' and then manually execute the 'run' command:\n"+
		"👉 \u001B[33mvim %s/manuscript.yaml\n"+
		"👉 \033[33mmanuscript-cli deploy %s/manuscript.yaml --env=local\n\n", manuscriptDir, manuscriptDir, manuscriptDir)
	log.Printf("\033[32mYou can now access your manuscript at http://localhost:%d\n", ms.Port)

	err := pkg.SaveConfig(manuscriptConfig, &pkg.Config{Manuscripts: []pkg.Manuscript{ms}})
	if err != nil {
		fmt.Printf("Failed to save manuscript config: %v", err)
		return
	}
}

func copyManuscriptFile(manuscriptDir, manuscriptPath string) error {
	// First read the entire source file
	content, err := os.ReadFile(manuscriptPath)
	if err != nil {
		return fmt.Errorf("failed to read source file: %w", err)
	}

	// Validate content
	if len(content) == 0 {
		return fmt.Errorf("source manuscript file is empty")
	}

	// Get destination path
	_, fileName := filepath.Split(manuscriptPath)
	destinationPath := filepath.Join(manuscriptDir, fileName)

	// Write the content atomically
	tempFile := destinationPath + ".tmp"
	if err := os.WriteFile(tempFile, content, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	// Rename temp file to final destination (atomic operation)
	if err := os.Rename(tempFile, destinationPath); err != nil {
		os.Remove(tempFile) // Clean up temp file if rename fails
		return fmt.Errorf("failed to rename temporary file: %w", err)
	}

	return nil
}

func ParseManuscriptYaml(manuscriptPath string) (pkg.Manuscript, error) {
	ms, err := pkg.ParseYAML(manuscriptPath)
	if err != nil {
		log.Fatalf("Error: Failed to parse manuscript yaml: %v", err)
		return pkg.Manuscript{}, err
	}
	return *ms, nil
}

func CheckManuscriptExist(ms pkg.Manuscript) error {
	dockers, err := pkg.RunDockerPs()
	if err != nil {
		log.Fatalf("Error: Failed to get docker ps: %v", err)
	}
	if len(dockers) == 0 {
		return nil
	}
	for _, d := range dockers {
		if d.Name == fmt.Sprintf("%s-jobmanager-1", ms.Name) {
			return fmt.Errorf("error: Manuscript [ %s ] already deployed, please change the name in the manuscript yaml file", ms.Name)
		}
	}
	return nil
}

func validateManuscriptFile(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read manuscript file: %w", err)
	}
	if len(content) == 0 {
		return fmt.Errorf("manuscript file is empty")
	}
	return nil
}
