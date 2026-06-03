// ==========================================================================================
// All Kubernetes interaction and API calls are handled in this abstracted service
// ==========================================================================================

package services

import (
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/benc-uk/go-rest-api/pkg/sse"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// Kubernetes is a service that connects to a Kubernetes cluster and provides access to its resources
type Kubernetes struct {
	dynamicClient     dynamic.Interface
	clientSet         kubernetes.Interface
	factory           dynamicinformer.DynamicSharedInformerFactory
	ClusterHost       string
	Mode              string // "in-cluster" or "out-of-cluster"
	KubeVersion       string
	UseEndpointSlices bool
}

// This is used by the SSE broker to send events to connected clients
type KubeEvent struct {
	// EventType is the type of event, e.g. "add", "update", "delete" or "ping"
	EventType EventTypeEnum
	// Object is the Kubernetes resource that triggered the event
	Object *unstructured.Unstructured
}

// EventTypeEnum is an enum for the type of event
type EventTypeEnum string

const (
	// AddEvent is triggered when a resource is added
	AddEvent EventTypeEnum = "add"
	// UpdateEvent is triggered when a resource is updated
	UpdateEvent EventTypeEnum = "update"
	// DeleteEvent is triggered when a resource is deleted
	DeleteEvent EventTypeEnum = "delete"
	// PingEvent is a heartbeat event to keep the connection alive
	PingEvent EventTypeEnum = "ping"
)

// NewKubernetes creates a new Kubernetes service instance
// - needs an SSE broker to send events to connected clients
func NewKubernetes(sseBroker *sse.Broker[KubeEvent], singleNamespace string, listOnly bool) (*Kubernetes, error) {
	var kubeConfig *rest.Config

	var err error

	mode := "out-of-cluster" // Default to out-of-cluster mode

	// In cluster connect using in-cluster "magic", else build config from .kube/config file
	if inCluster() {
		log.Println("⚓ Running in cluster, will try to use cluster config")

		kubeConfig, err = rest.InClusterConfig()
		mode = "in-cluster"
	} else {
		// Default location for kubeconfig file is $HOME/.kube/config
		kubeconfigFile := filepath.Join(os.Getenv("HOME"), ".kube", "config")

		// If KUBECONFIG environment variable is set, use that instead
		if os.Getenv("KUBECONFIG") != "" {
			kubeconfigFile = os.Getenv("KUBECONFIG")
		}

		log.Println("🏠 Running outside cluster, will use config file:", kubeconfigFile)
		kubeConfig, err = clientcmd.BuildConfigFromFlags("", kubeconfigFile)
	}

	if err != nil {
		return nil, err
	}

	log.Println("🌐 Kubernetes host:", kubeConfig.Host)

	// DiscoveryClient is used to discover the Kubernetes API resources
	// It is used to check the server version and capabilities
	discClient, err := discovery.NewDiscoveryClientForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	// Validate the connection to the Kubernetes API by checking the server version
	serverVersion, err := discClient.ServerVersion()
	if err != nil {
		log.Println("⛔ Failed to connect to Kubernetes API", err)
		return nil, err
	} else {
		log.Println("✅ Connected to Kubernetes API, version:", serverVersion.String())
	}

	useEndpointSlices := false

	// If the server version is 1.33 or higher, we will use EndpointSlices instead of Endpoints
	// See https://kubernetes.io/blog/2025/04/24/endpoints-deprecation/
	if serverVersion.Major == "1" && serverVersion.Minor >= "33" {
		log.Println("🔄 Kubernetes version > 1.32 Using EndpointSlices for service endpoints")

		useEndpointSlices = true
	}

	// Use the dynamic client to interact with the Kubernetes API
	// This allows us to work with any resource type without needing to know the schema in advance
	dynamicClient, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	// ClientSet is the standard Kubernetes client for interacting with the API
	// It is used for operations that require the full client, such as getting logs
	clientSet, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, err
	}

	namespace := coreV1.NamespaceAll // Work in all namespaces
	if singleNamespace != "" {
		namespace = singleNamespace
		log.Println("🔑 Authorised for a single namespace:", namespace)
	}

	log.Println("👀 Setting up resource watchers...")

	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		dynamicClient, time.Minute, namespace, nil)

	// Add listening event handlers for ALL resources we want to track
	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "networking.k8s.io",
		Version: "v1", Resource: "ingresses"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "",
		Version: "v1", Resource: "persistentvolumeclaims"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "autoscaling", Version: "v2",
		Resource: "horizontalpodautoscalers"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	if useEndpointSlices {
		_, _ = factory.ForResource(schema.GroupVersionResource{Group: "discovery.k8s.io",
			Version: "v1", Resource: "endpointslices"}).
			Informer().
			AddEventHandler(getHandlerFuncs(sseBroker))
	} else {
		_, _ = factory.ForResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "endpoints"}).
			Informer().
			AddEventHandler(getHandlerFuncs(sseBroker))
	}

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	_, _ = factory.ForResource(schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}).
		Informer().
		AddEventHandler(getHandlerFuncs(sseBroker))

	// List-only mode: skip starting the informers and the blocking cache sync.
	// The handlers/informers above are created but inert (no list/watch network
	// calls happen until factory.Start), so the HTTP server binds immediately
	// even on RBAC-restricted clusters. /api/fetch and /updates read empty
	// caches in this mode and are not used by the client here.
	if !listOnly {
		factory.Start(context.Background().Done())
		factory.WaitForCacheSync(context.Background().Done())
	} else {
		log.Println("📋 List-only mode: skipping resource watchers")
	}

	return &Kubernetes{
		dynamicClient:     dynamicClient,
		clientSet:         clientSet, // Deprecated, use client instead
		factory:           factory,
		ClusterHost:       kubeConfig.Host,
		Mode:              mode,
		UseEndpointSlices: useEndpointSlices,
		KubeVersion:       serverVersion.String(),
	}, nil
}

// Get namespaces
func (k *Kubernetes) GetNamespaces() ([]string, error) {
	out := []string{}

	// Use the dynamicClient to get the list of namespaces
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

	l, err := k.dynamicClient.Resource(gvr).List(context.TODO(), metaV1.ListOptions{})
	if err != nil {
		log.Println("💥 Failed to get namespaces:", err)
		return nil, err
	}

	// Iterate over the namespaces and add them to the list
	for _, ns := range l.Items {
		out = append(out, ns.GetName())
	}

	return out, nil
}

// Validate if a namespace exists in the cluster
func (k *Kubernetes) CheckNamespaceExists(ns string) bool {
	gvr := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

	// Try to get the namespace
	_, err := k.dynamicClient.Resource(gvr).Get(context.TODO(), ns, metaV1.GetOptions{})

	return err == nil
}

// Retrieves all resources in a specific namespace and returns them in a big ol' map.
// Reads from the in-memory informer cache populated by NewKubernetes, avoiding
// per-request List calls against the API server.
func (k *Kubernetes) FetchNamespace(ns string) (map[string][]unstructured.Unstructured, error) {
	if ns == "" {
		return nil, errors.New("namespace is empty")
	}

	type kindGVR struct {
		key string
		gvr schema.GroupVersionResource
	}

	kinds := []kindGVR{
		{"pods", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}},
		{"services", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}},
		{"deployments", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}},
		{"replicasets", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}},
		{"statefulsets", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "statefulsets"}},
		{"daemonsets", schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "daemonsets"}},
		{"jobs", schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "jobs"}},
		{"cronjobs", schema.GroupVersionResource{Group: "batch", Version: "v1", Resource: "cronjobs"}},
		{"ingresses", schema.GroupVersionResource{Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"}},
		{"configmaps", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}},
		{"secrets", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}},
		{"persistentvolumeclaims", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "persistentvolumeclaims"}},
		{"events", schema.GroupVersionResource{Group: "", Version: "v1", Resource: "events"}},
		{"horizontalpodautoscalers", schema.GroupVersionResource{Group: "autoscaling", Version: "v2", Resource: "horizontalpodautoscalers"}},
	}

	data := make(map[string][]unstructured.Unstructured, len(kinds)+1)
	for _, kg := range kinds {
		data[kg.key] = k.listFromCache(kg.gvr, ns)
	}

	if k.UseEndpointSlices {
		data["endpointslices"] = k.listFromCache(
			schema.GroupVersionResource{Group: "discovery.k8s.io", Version: "v1", Resource: "endpointslices"}, ns)
	} else {
		data["endpoints"] = k.listFromCache(
			schema.GroupVersionResource{Group: "", Version: "v1", Resource: "endpoints"}, ns)
	}

	// Clean up the managed fields and redact sensitive data
	for _, items := range data {
		for i := range items {
			// Managed fields are simply clutter
			items[i].SetManagedFields(nil)

			// Loop through the data field of Secrets & ConfigMaps and redact it
			if items[i].GetKind() == "Secret" || items[i].GetKind() == "ConfigMap" {
				if data, ok := items[i].Object["data"].(map[string]interface{}); ok {
					for k := range data {
						data[k] = "*REDACTED*"
					}
				}
			}
		}
	}

	return data, nil
}

// listFromCache returns deep-copied resources of the given GVR scoped to the
// namespace, read from the shared informer's local cache. Returns an empty
// slice (never nil) if the informer is unregistered or the cache is empty,
// preserving the JSON shape returned by FetchNamespace.
func (k *Kubernetes) listFromCache(gvr schema.GroupVersionResource, ns string) []unstructured.Unstructured {
	out := []unstructured.Unstructured{}

	if k.factory == nil {
		return out
	}

	informer := k.factory.ForResource(gvr).Informer()
	for _, obj := range informer.GetIndexer().List() {
		u, ok := obj.(*unstructured.Unstructured)
		if !ok {
			continue
		}

		if u.GetNamespace() != ns {
			continue
		}

		out = append(out, *u.DeepCopy())
	}

	return out
}

// Generic function to list resources from a specific namespace
func (k *Kubernetes) GetResources(ns string, grp string, ver string, res string) ([]unstructured.Unstructured, error) {
	gvr := schema.GroupVersionResource{Group: grp, Version: ver, Resource: res}

	l, err := k.dynamicClient.Resource(gvr).Namespace(ns).List(context.TODO(), metaV1.ListOptions{Limit: 1000})
	if err != nil {
		log.Printf("💥 Failed to get %s %v", res, err)
		return nil, err
	}

	return l.Items, nil
}

// Retrieves the logs of a specific pod in a given namespace
func (k *Kubernetes) GetPodLogs(ns, podName string, lineCount int) (string, error) {
	if ns == "" || podName == "" {
		return "", errors.New("namespace or pod name is empty")
	}

	if lineCount <= 0 {
		lineCount = 100 // Default to 100 lines if not specified
	}

	// Get the lines of logs from the pod
	req := k.clientSet.CoreV1().Pods(ns).GetLogs(podName, &coreV1.PodLogOptions{
		TailLines: &[]int64{int64(lineCount)}[0], // We pass in how many lines we want to get
	})

	logs, err := req.DoRaw(context.TODO())
	if err != nil {
		log.Printf("💥 Failed to get logs for pod %s in namespace %s: %v", podName, ns, err)
		return "", err
	}

	return string(logs), nil
}

func inCluster() bool {
	// Check if the application is running inside a Kubernetes cluster
	// This is a simple check and may not be foolproof
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		return true
	}

	return false
}

// getHandlerFuncs returns the event handlers for the Kubernetes informers, which send events through the SSE broker.
//
// Each handler DeepCopies the incoming object before mutating it. The informer
// hands out a pointer to the object held in the shared cache, and the listener
// callbacks run on a goroutine pool concurrent with the informer's own delta
// processing. Mutating the cached pointer races against `MetaNamespaceIndexFunc`
// reads that happen inside the informer's `threadSafeMap` updates (Go's
// runtime catches this as `fatal error: concurrent map read and map write`).
// The DeepCopy makes the SSE pipeline operate on a private clone, leaving the
// cache pristine for the Pack-2 cache-read path used by FetchNamespace.
func getHandlerFuncs(b *sse.Broker[KubeEvent]) cache.ResourceEventHandlerFuncs {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			orig, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}

			namespace := orig.GetNamespace()
			if namespace == "" {
				return
			}

			u := orig.DeepCopy()
			u.SetManagedFields(nil)
			b.SendToGroup(namespace, KubeEvent{
				EventType: AddEvent,
				Object:    u,
			})
		},

		UpdateFunc: func(oldObj, newObj interface{}) {
			orig, ok := newObj.(*unstructured.Unstructured)
			if !ok {
				return
			}

			namespace := orig.GetNamespace()
			if namespace == "" {
				return
			}

			u := orig.DeepCopy()
			u.SetManagedFields(nil)
			b.SendToGroup(namespace, KubeEvent{
				EventType: UpdateEvent,
				Object:    u,
			})
		},

		DeleteFunc: func(obj interface{}) {
			orig, ok := obj.(*unstructured.Unstructured)
			if !ok {
				return
			}

			namespace := orig.GetNamespace()
			if namespace == "" {
				return
			}

			u := orig.DeepCopy()
			u.SetManagedFields(nil)
			b.SendToGroup(namespace, KubeEvent{
				EventType: DeleteEvent,
				Object:    u,
			})
		},
	}
}
