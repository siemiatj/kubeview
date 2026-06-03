// ==========================================================================================
// The backend API for KubeView, handling requests and serving data
// ==========================================================================================

package main

import (
	"log"

	"github.com/benc-uk/go-rest-api/pkg/api"
	"github.com/benc-uk/kubeview/server/services"
)

// This is the core struct for the server & API
type KubeviewAPI struct {
	*api.Base
	kubeService *services.Kubernetes
	eventBroker KubeEventBroker
	config      Config
}

type NamespaceListResult struct {
	Namespaces []string `json:"namespaces"`
	// CanListNamespaces is false when the identity lacks cluster-scope
	// namespaces.list permission (RBAC-restricted clusters). In that case
	// Namespaces is empty and the client must let the user type a namespace.
	CanListNamespaces bool `json:"canListNamespaces"`
	// We munge a couple of extra fields into the API response
	// This saves us from having to make a separate request for the version and build info
	ClusterHost    string `json:"clusterHost"`
	Version        string `json:"version"`
	BuildInfo      string `json:"buildInfo"`
	Mode           string `json:"mode"`
	PodLogsEnabled bool   `json:"podLogsEnabled"`
}

func NewKubeviewAPI(conf Config) *KubeviewAPI {
	broker := newKubeEventBroker(conf)

	// Create a new Kubernetes service instance, which will connect to the cluster
	kubeSvc, err := services.NewKubernetes(broker.Broker, conf.SingleNamespace, conf.ListNamespacesOnly)
	if err != nil {
		log.Fatalf("💥 Error connecting to Kubernetes, system will exit")
	}

	// Our API struct is a wrapper around the base API functionality
	return &KubeviewAPI{
		api.NewBase("kubeview", version, buildInfo, true),
		kubeSvc,
		broker,
		conf,
	}
}
