package manager

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sjson "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"os"
	"path/filepath"
	"strings"

	gabs "github.com/Jeffail/gabs/v2"
	"github.com/rancher/support-bundle-kit/pkg/utils"
	k8syaml "sigs.k8s.io/yaml"
)

type Cluster struct {
	sbm *SupportBundleManager
}

func NewCluster(ctx context.Context, sbm *SupportBundleManager) *Cluster {
	return &Cluster{
		sbm: sbm,
	}
}

func (c *Cluster) GenerateClusterBundle(bundleDir string) (string, error) {
	logrus.Debug("generating cluster bundle...")
	namespace, err := c.sbm.k8s.GetNamespace(c.sbm.PodNamespace)
	if err != nil {
		return "", errors.Wrap(err, "cannot get harvester namespace")
	}
	kubeVersion, err := c.sbm.k8s.GetKubernetesVersion()
	if err != nil {
		return "", errors.Wrap(err, "cannot get kubernetes version")
	}

	sb, err := c.sbm.state.GetSupportBundle(c.sbm.PodNamespace, c.sbm.BundleName)
	if err != nil {
		return "", errors.Wrap(err, "cannot get support bundle")
	}

	bundleMeta := &BundleMeta{
		ProjectName:          "Harvester",
		ProjectVersion:       c.sbm.harvester.GetSettingValue("server-version"),
		BundleVersion:        BundleVersion,
		KubernetesVersion:    kubeVersion.GitVersion,
		ProjectNamespaceUUID: string(namespace.UID),
		BundleCreatedAt:      utils.Now(),
		IssueURL:             sb.Spec.IssueURL,
		IssueDescription:     sb.Spec.Description,
	}

	bundleName := fmt.Sprintf("supportbundle_%s_%s.zip",
		bundleMeta.ProjectNamespaceUUID,
		strings.Replace(bundleMeta.BundleCreatedAt, ":", "-", -1))

	errLog, err := os.Create(filepath.Join(bundleDir, "bundleGenerationError.log"))
	if err != nil {
		logrus.Errorf("Failed to create bundle generation log")
		return "", err
	}
	defer errLog.Close()

	metaFile := filepath.Join(bundleDir, "metadata.yaml")
	encodeToYAMLFile(bundleMeta, metaFile, errLog, nil)

	yamlsDir := filepath.Join(bundleDir, "yamls")
	c.generateSupportBundleYAMLs(yamlsDir, errLog)

	logsDir := filepath.Join(bundleDir, "logs")
	c.generateSupportBundleLogs(logsDir, errLog)

	return bundleName, nil
}

func (c *Cluster) generateSupportBundleYAMLs(yamlsDir string, errLog io.Writer) {
	// Cluster scope
	globalDir := filepath.Join(yamlsDir, "cluster")
	c.generateKubernetesClusterYAMLs(globalDir, errLog)
	//c.generateDiscoveredClusterYAMLs(globalDir, errLog)

	// Namespaced scope: k8s resources
	namespaces := []string{"default", "kube-system", "cattle-system"}
	namespaces = append(namespaces, c.sbm.Namespaces...)
	for _, namespace := range namespaces {
		namespacedDir := filepath.Join(yamlsDir, "namespaced", namespace)
		c.generateKubernetesNamespacedYAMLs(namespace, namespacedDir, errLog)
	}

	// Namespaced scope: harvester cr
	namespaces = []string{"default"}
	namespaces = append(namespaces, c.sbm.Namespaces...)
	for _, namespace := range namespaces {
		namespacedDir := filepath.Join(yamlsDir, "namespaced", namespace)
		c.generateDiscoveredNamespacedYAMLs(namespace, namespacedDir, errLog)
	}
}

type NamespacedGetter func(string) (runtime.Object, error)

func wrap(ns string, getter NamespacedGetter) GetRuntimeObjectListFunc {
	wrapped := func() (runtime.Object, error) {
		return getter(ns)
	}
	return wrapped
}

type DataModifier struct {
	modifiers map[string]string
}

func (c *Cluster) generateKubernetesClusterYAMLs(dir string, errLog io.Writer) {
	toDir := filepath.Join(dir, "kubernetes")
	getListAndEncodeToYAML("nodes", c.sbm.k8s.GetAllNodesList, toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "v1",
			"kind":      "Node",
			"status.nodeInfo.machineID": "null",
		},
	})
	getListAndEncodeToYAML("volumeattachments", c.sbm.k8s.GetAllVolumeAttachments, toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "metrics.k8s.io/v1beta1",
			"kind": "VolumeAttachment",
		},
	})
	getListAndEncodeToYAML("nodemetrics", c.sbm.k8sMetrics.GetAllNodeMetrics, toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "storage.k8s.io/v1",
			"kind": "NodeMetrics",
		},
	})
}

func (c *Cluster) generateKubernetesNamespacedYAMLs(namespace string, dir string, errLog io.Writer) {
	toDir := filepath.Join(dir, "kubernetes")
	getListAndEncodeToYAML("events", wrap(namespace, c.sbm.k8s.GetAllEventsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "v1",
			"kind": "Event",
		},
	})
	getListAndEncodeToYAML("pods", wrap(namespace, c.sbm.k8s.GetAllPodsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "v1",
			"kind": "Pod",
			"status.conditions.lastProbeTime": "null",
		},
	})
	getListAndEncodeToYAML("services", wrap(namespace, c.sbm.k8s.GetAllServicesList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "v1",
			"kind": "Service",
		},
	})
	getListAndEncodeToYAML("deployments", wrap(namespace, c.sbm.k8s.GetAllDeploymentsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "apps/v1",
			"kind": "Deployment",
		},
	})
	getListAndEncodeToYAML("daemonsets", wrap(namespace, c.sbm.k8s.GetAllDaemonSetsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "apps/v1",
			"kind": "DaemonSet",
		},
	})
	getListAndEncodeToYAML("statefulsets", wrap(namespace, c.sbm.k8s.GetAllStatefulSetsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "apps/v1",
			"kind": "StatefulSet",
		},
	})
	getListAndEncodeToYAML("jobs", wrap(namespace, c.sbm.k8s.GetAllJobsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "batch/v1",
			"kind": "Job",
		},
	})
	getListAndEncodeToYAML("cronjobs", wrap(namespace, c.sbm.k8s.GetAllCronJobsList), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "batch/v1beta1",
			"kind": "CronJob",
		},
	})
	getListAndEncodeToYAML("configmaps", wrap(namespace, c.sbm.k8s.GetAllConfigMaps), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "v1",
			"kind": "ConfigMap",
		},
	})
	getListAndEncodeToYAML("podmetrics", wrap(namespace, c.sbm.k8sMetrics.GetAllPodMetrics), toDir, errLog, &DataModifier{
		modifiers: map[string]string{
			"apiVersion": "metrics.k8s.io/v1beta1",
			"kind": "PodMetrics",
		},
	})
}

func (c *Cluster) generateDiscoveredNamespacedYAMLs(namespace string, dir string, errLog io.Writer) {

	objs := c.sbm.discovery.ResourcesForNamespace(namespace)

	for name, obj := range objs {
		file := filepath.Join(dir, name+".yaml")
		encodeToYAMLFile(obj, file, errLog, nil)
	}
}

func (c *Cluster) generateDiscoveredClusterYAMLs(dir string, errLog io.Writer) {
	objs := c.sbm.discovery.ResourcesForCluster()

	for name, obj := range objs {
		file := filepath.Join(dir, name+".yaml")
		encodeToYAMLFile(obj, file, errLog, nil)
	}
}

func encodeToYAMLFile(obj interface{}, path string, errLog io.Writer, dataModifier *DataModifier) {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(errLog, "Support Bundle: failed to generate %v: %v\n", path, err)
		}
	}()
	err = os.MkdirAll(filepath.Dir(path), os.FileMode(0755))
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return
	}
	defer f.Close()

	scheme := runtime.NewScheme()
	separator := []byte("---\n")

	switch v := obj.(type) {
	case runtime.Object:
		serializer := k8sjson.NewSerializerWithOptions(k8sjson.DefaultMetaFactory, scheme, scheme, k8sjson.SerializerOptions{
			Yaml:   false,
			Pretty: false,
			Strict: false,
		})
		buf := new(bytes.Buffer)
		if err = serializer.Encode(v, buf); err != nil {
			return
		}
		jsonParsed, err := gabs.ParseJSON(buf.Bytes())
		if err != nil {
			return
		}

		for _, child := range jsonParsed.S("items").Children() {
			//for k, v := range dataModifier.appends {
			//	_, error := child.Set(v, k)
			//	if error != nil {
			//		logrus.Infof("can not set: %s, %s", k, v)
			//	}
			//}

			for k, v := range dataModifier.modifiers {
				_, error := child.SetP(v, k)
				if error != nil {
					logrus.Infof("can not setP: %s, %s", k, v)
				}
			}

			data, err := k8syaml.JSONToYAML(child.Bytes())
			if err != nil {
				logrus.Info("can not convert back to yaml")
			}
			_, err = f.Write(separator)
			_, err = f.Write(data)
			if err != nil {
				logrus.Info("can not write to file")
			}
		}
	default:
		encoder := yaml.NewEncoder(f)
		if err = encoder.Encode(obj); err != nil {
			return
		}
		if err = encoder.Close(); err != nil {
			return
		}
	}
}

type GetRuntimeObjectListFunc func() (runtime.Object, error)

func getListAndEncodeToYAML(name string, getListFunc GetRuntimeObjectListFunc, yamlsDir string, errLog io.Writer, dataModifier *DataModifier) {
	obj, err := getListFunc()
	if err != nil {
		fmt.Fprintf(errLog, "Support Bundle: failed to get %v: %v\n", name, err)
	}
	encodeToYAMLFile(obj, filepath.Join(yamlsDir, name+".yaml"), errLog, dataModifier)
}

func (c *Cluster) generateSupportBundleLogs(logsDir string, errLog io.Writer) {
	namespaces := []string{"default", "kube-system", "cattle-system"}
	namespaces = append(namespaces, c.sbm.Namespaces...)

	for _, ns := range namespaces {
		list, err := c.sbm.k8s.GetAllPodsList(ns)
		if err != nil {
			fmt.Fprintf(errLog, "Support bundle: cannot get pod list: %v\n", err)
			return
		}
		podList, ok := list.(*corev1.PodList)
		if !ok {
			fmt.Fprintf(errLog, "BUG: Support bundle: didn't get pod list\n")
			return
		}
		for _, pod := range podList.Items {
			podName := pod.Name
			podDir := filepath.Join(logsDir, ns, podName)
			for _, container := range pod.Spec.Containers {
				req := c.sbm.k8s.GetPodContainerLogRequest(ns, podName, container.Name)
				logFileName := filepath.Join(podDir, container.Name+".log")
				stream, err := req.Stream(c.sbm.context)
				if err != nil {
					fmt.Fprintf(errLog, "BUG: Support bundle: cannot get log for pod %v container %v: %v\n",
						podName, container.Name, err)
					continue
				}
				streamLogToFile(stream, logFileName, errLog)
				stream.Close()
			}
		}
	}
}

func streamLogToFile(logStream io.ReadCloser, path string, errLog io.Writer) {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(errLog, "Support Bundle: failed to generate %v: %v\n", path, err)
		}
	}()
	err = os.MkdirAll(filepath.Dir(path), os.FileMode(0755))
	if err != nil {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = io.Copy(f, logStream)
	if err != nil {
		return
	}
}
