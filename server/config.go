// ==========================================================================================
// Server configuration via environment variables
// ==========================================================================================

package main

import (
	"os"
	"strconv"
)

// Config holds the configuration for the system
type Config struct {
	Port               int
	NameSpaceFilter    string
	SingleNamespace    string
	ListNamespacesOnly bool
	Debug              bool
	EnablePodLogs      bool
}

// Parse the environment variables and return a Config struct
// Also provides default values if the environment variables are not set
func getConfig() Config {
	// Default values
	port := 8000
	nameSpaceFilter := ""
	singleNamespace := ""
	listNamespacesOnly := false
	debug := false
	enablePodLogs := true

	if portEnv := os.Getenv("PORT"); portEnv != "" {
		if p, err := strconv.Atoi(portEnv); err == nil {
			port = p
		}
	}

	if s := os.Getenv("SINGLE_NAMESPACE"); s != "" {
		singleNamespace = s
	}

	if s := os.Getenv("NAMESPACE_FILTER"); s != "" {
		nameSpaceFilter = s
	}

	// List-only bootstrap mode: serve the namespace list without setting up any
	// informers (no cluster-wide list/watch), so the HTTP server binds even on
	// RBAC-restricted clusters. The client uses this to populate the namespace
	// picker, then restarts the sidecar scoped via SINGLE_NAMESPACE.
	if s := os.Getenv("LIST_NAMESPACES_ONLY"); s != "" {
		listNamespacesOnly = true
	}

	if s := os.Getenv("DISABLE_POD_LOGS"); s != "" {
		if enable, err := strconv.ParseBool(s); err == nil {
			enablePodLogs = !enable
		}
	}

	if debugEnv := os.Getenv("DEBUG"); debugEnv != "" {
		debug, _ = strconv.ParseBool(debugEnv)
	}

	return Config{
		Port:               port,
		NameSpaceFilter:    nameSpaceFilter,
		SingleNamespace:    singleNamespace,
		ListNamespacesOnly: listNamespacesOnly,
		Debug:              debug,
		EnablePodLogs:      enablePodLogs,
	}
}
