package commands

import (
	"fmt"
	"log"
	"manuscript-core/pkg"
	"os"

	"github.com/spf13/cobra"
)

// Define global CLI variables
var (
	env     string
	version = "1.1.0"
)

// Execute runs the CLI commands
func Execute(args []string) error {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return nil
}

// Manuscript CLI commands
// Each command has a short description, long description, and example usage
// Commands are grouped logically and added to the root command by addCommands()
// Flags are added to commands as needed by configureFlags()

// `init` command creates a new manuscript project
var initCmd = &cobra.Command{
	Use:     "init",
	Aliases: []string{"ini", "in", "i"},
	Short:   "Initialize and start local manuscript containers",
	Long: `📦 Initialize a new Manuscript project

Setup Steps:
✨ Create project structure & configs
🐳 Initialize Docker containers
🔌 Configure database connections
🚀 Set up GraphQL endpoints

You'll be prompted to select:
• Project name     ✅
• Chain type       ✅
• Tables           ✅
• Output format    ✅`,
	Example: `>> manuscript-cli i
>> manuscript-cli ini
>> manuscript-cli init`,
	Args: cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		InitManuscript()
	},
}

// `list` command lists all manuscript jobs
var jobListCmd = &cobra.Command{
	Use:     "list [directory]",
	Aliases: []string{"ls"},
	Short:   "List all manuscript jobs",
	Long: `📋 View all running manuscript jobs

Each job shows:
🔷 Manuscript name
🔷 Status
🔷 Start time & duration
🔷 GraphQL endpoint

Status indicators:
🟢 Running - Job is active and processing data
🟡 Warning - Job needs attention
🔴 Failed - Job encountered an error
⚫ Stopped - Job was stopped
⚪️ Other - Various other states

Usage:
- Run without arguments to check default directory
- Specify a directory path to check manuscripts in that location`,
	Example: `>> manuscript-cli ls
>> manuscript-cli list /path/to/manuscripts`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		var dir string
		// if no args, use default manuscript directory
		if len(args) == 0 {
			config, err := pkg.LoadConfig(manuscriptConfig)
			if err != nil {
				log.Fatalf("Error: Failed to load config: %v", err)
			}
			dir = fmt.Sprintf("%s/%s", config.BaseDir, manuscriptBaseName)
		} else {
			dir = args[0] // use specified directory if user input
		}
		ListJobs(dir)
	},
}

// `stop` command stops a manuscript job's containers from
var jobStopCmd = &cobra.Command{
	Use:   "stop <job_name>",
	Short: "Stop a manuscript job",
	Long: `🛑 Stop a running manuscript job

Actions:
✨ Graceful job termination
🧹 Docker container cleanup
💾 Config & data preservation
🔓 Port release

Note: Restart jobs using 'deploy' command`,
	Example: `>> manuscript-cli stop <my-manuscript1> <my-manuscript2>`,
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		JobStop(args[0])
	},
}

// `logs` command shows logs of a manuscript job
var jobLogsCmd = &cobra.Command{
	Use:   "logs <job_name>",
	Short: "View logs of a manuscript job",
	Long: `📋 View manuscript job logs in real-time

Shows:
🔷 Startup events
🔷 Processing stats
🔷 Error tracking
🔷 Performance data
🔷 Connection info`,
	Example: `>> manuscript-cli logs <my-manuscript>`,
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		JobLogs(args[0])
	},
}

// `chat` command gives Text-to-SQL chat interface with the dataset AI
var chatCmd = &cobra.Command{
	Use:     "chat <job_name>",
	Aliases: []string{"c"},
	Short:   "Chat with the dataset AI",
	Long: `🤖 Interactive AI Chat for Your Dataset

Features:
🔷 Natural language to SQL queries
🔷 Multi-AI provider support
🔷 Interactive refinement
🔷 Real-time execution
🔷 Formatted results

AI Providers:
✨ OpenAI (ChatGPT)
✨ Google Gemini
✨ Gaia

Required Env Vars:
🔑 OPENAI_API_KEY (ChatGPT)
🔑 GEMINI_API_KEY (Google Gemini)
🔑 OPENAI_API_BASE (optional/custom)`,
	Example: `>> manuscript-cli chat my-manuscript
>> manuscript-cli c my-manuscript`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		Chat(args[0])
	},
}

// `deploy` command deploys an existing manuscript.yaml file
var deployManuscript = &cobra.Command{
	Use:     "deploy <manuscript-file>",
	Aliases: []string{"d"},
	Short:   "Deploy Manuscript locally or to Chainbase network",
	Long: `🚀 Deploy Your Manuscript Configuration

Deployment Steps:
🔍 Validate configuration
🔧 Check resources
🏗️ Setup infrastructure
🚀 Deploy containers
🔌 Initialize connections
☑️ Verify deployment

Environments:
🔷 local    - Deploy to Docker
🔷 chainbase - Deploy to Network (soon)

Requirements:
📋 manuscript.yaml
🔑 Environment variables
🐳 Running Docker (local only)`,
	Example: `>> manuscript-cli deploy config.yaml --env=local
>> manuscript-cli d config.yaml --env=local`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		switch env {
		case "local":
			DeployManuscript(args)
		case "chainbase":
			fmt.Println("Deploying to Chainbase network...coming soon!")
		default:
			fmt.Println("Unknown environment. Please specify --env=local or --env=chainbase")
		}
	},
}

// `version` command shows the version of manuscript-cli
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version of manuscript-cli",
	Long: `ℹ️  Manuscript CLI Version Information

Shows:
🛠️ Manuscript Core version
🏗️ Build info
💻 System information
🐳 Docker environment details`,
	Example: `>> manuscript-cli version`,
	Run: func(cmd *cobra.Command, args []string) {
		verbosity, _ := cmd.Flags().GetBool("verbose")
		showVersion(verbosity)
	},
}

func init() {
	// Configure help from help_template.go
	configureHelp(rootCmd)

	// Ensure commands show up as added instead of alphabetically
	cobra.EnableCommandSorting = false

	// Add CLI commands in logical groups
	addCommands()

	// Add flags to commands
	configureFlags()

	// Disable the default completion command
	rootCmd.CompletionOptions.HiddenDefaultCmd = true
}

func addCommands() {

	// Manuscript creation & deployment commands
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(deployManuscript)

	// Job management commands
	rootCmd.AddCommand(jobListCmd)
	rootCmd.AddCommand(jobStopCmd)
	rootCmd.AddCommand(jobLogsCmd)

	// Interactive commands
	rootCmd.AddCommand(chatCmd)

	// Utility commands
	rootCmd.AddCommand(versionCmd)

}

func configureFlags() {
	// Configure deployment flags
	deployManuscript.Flags().StringVar(&env, "env", "", "Specify the environment to deploy (local or chainbase)")
	deployManuscript.MarkFlagRequired("env")

	// Configure version command flags
	versionCmd.Flags().BoolP("verbose", "v", false, "Display detailed version information")
}

var rootCmd = &cobra.Command{
	Use: "manuscript-cli [command]",
}
