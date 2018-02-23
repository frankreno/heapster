package sumologic

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
)

const (
	Carbon2ContentType = "application/vnd.sumologic.carbon2"
)

type Sink struct {
	url string
	dimensions map[string]string
	metadata map[string]string
	sync.RWMutex
}

func NewSumoLogicSink(uri *url.URL) (core.DataSink, error) {
	dimensionsParam := uri.Query().Get("dimensions")
	dimensions := make(map[string]string)
	if dimensionsParam != "" {
		for _, value := range strings.Split(dimensionsParam, ",") {
			dimension := strings.Split(value, "=")
			dimensions[dimension[0]] = dimension[1]
		}
	}
	metadataParam := uri.Query().Get("metadata")
	metadata := make(map[string]string)
	if dimensionsParam != "" {
		for _, value := range strings.Split(metadataParam, ",") {
			data := strings.Split(value, "=")
			metadata[data[0]] = data[1]
		}
	}
	httpSourceUrl := url.URL{Scheme:uri.Scheme, Host:uri.Host, Path:uri.Path}
	return &Sink{url: httpSourceUrl.String(), dimensions: dimensions, metadata: metadata}, nil
}

func (sink *Sink) Name() string {
	return "Sumo Logic Sink"
}

func (sink *Sink) Stop() {
	glog.Info("stopping Sumo Logic Sink")
}

func (sink *Sink) ExportData(dataBatch *core.DataBatch) {
	sink.Lock()
	defer sink.Unlock()
	var metrics []Carbon2Metric
	for _, metricSet := range dataBatch.MetricSets {
		var m *Metric
		for metricName, metricValue := range metricSet.MetricValues {
			m = &Metric{
				name:      metricName,
				value:     metricValue,
				labels:    metricSet.Labels,
				timestamp: dataBatch.Timestamp.Unix(),
			}
			metrics = append(metrics, m.Metric(sink))
		}
		for _, metric := range metricSet.LabeledMetrics {
			if value := metric.GetValue(); value != nil {
				labels := make(map[string]string)
				for k, v := range metricSet.Labels {
					labels[k] = v
				}
				for k, v := range metric.Labels {
					labels[k] = v
				}
				m = &Metric{
					name:      metric.Name,
					value:     metric.MetricValue,
					labels:    labels,
					timestamp: dataBatch.Timestamp.Unix(),
				}
				metrics = append(metrics, m.Metric(sink))
			}
		}
	}
	glog.Infof("Sending %d metrics to Sumo Logic", len(metrics))
	sendToSumoLogic(sink.url, metrics)
}

func sendToSumoLogic(url string, metrics []Carbon2Metric) {
	var buffer bytes.Buffer
	for _, metric := range metrics {
		buffer.WriteString(fmt.Sprintf("%s\n", metric.String()))
	}
	client := &http.Client{}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(buffer.String())))
	if err != nil {
		glog.Fatalf("error creating request: %s", err)
	}
	req.Close = true
	req.Header.Set("Content-Type", Carbon2ContentType)
	resp, err := client.Do(req)
	if err != nil {
		glog.Fatalf("error executing request: %s", err)
	}
	if resp.StatusCode != 200 {
		glog.Errorf("unable to send data to Sumo Logic, got back status code %d. response:%s", resp.StatusCode, resp.Body)
	}
	defer resp.Body.Close()
}

type Metric struct {
	name      string
	value     core.MetricValue
	labels    map[string]string
	timestamp int64
}

func (m *Metric) IntrinsicTags() map[string]string {
	intrinsicTags := make(map[string]string)
	var metricPath string
	if resourceId, ok := m.labels["resourceId"]; ok {
		nameParts := strings.Split(m.name, "/")
		section, parts := nameParts[0], nameParts[1:]
		metricPath = strings.Join(append([]string{section, resourceId}, parts...), ".")
	} else {
		metricPath = m.name
	}
	metricPath = strings.Replace(metricPath, "/", ".", -1)
	intrinsicTags["metric"] = metricPath
	if t, ok := m.labels[core.LabelMetricSetType.Key]; ok {
		switch t {
		case core.MetricSetTypePodContainer:
			intrinsicTags["node"] = m.labels[core.LabelHostname.Key]
			intrinsicTags["namespace"] = m.labels[core.LabelNamespaceName.Key]
			intrinsicTags["pod"] = m.labels[core.LabelPodName.Key]
			intrinsicTags["container"] = m.labels[core.LabelContainerName.Key]
		case core.MetricSetTypeSystemContainer:
			intrinsicTags["node"] = m.labels[core.LabelHostname.Key]
			intrinsicTags["sys-containers"] = m.labels[core.LabelContainerName.Key]
		case core.MetricSetTypePod:
			intrinsicTags["node"] = m.labels[core.LabelHostname.Key]
			intrinsicTags["namespace"] = m.labels[core.LabelNamespaceName.Key]
			intrinsicTags["pod"] = m.labels[core.LabelPodName.Key]
		case core.MetricSetTypeNamespace:
			intrinsicTags["namespace"] = m.labels[core.LabelNamespaceName.Key]
		case core.MetricSetTypeNode:
			intrinsicTags["node"] = m.labels[core.LabelHostname.Key]
		case core.MetricSetTypeCluster:
		default:
			glog.Infof("Unknown metric type %s", t)
		}
	}
	return intrinsicTags
}

func (m *Metric) MetaTags()  map[string]string {
	metaTags := make(map[string]string)
	labels := strings.Split(m.labels[core.LabelLabels.Key], ",")
	for _, s := range labels {
		label := strings.Split(s, ":")
		if len(label) == 2 {
			if label[1] != "" {
				key := label[0]
				value := label[1]
				metaTags[key] = value
			}
		}
	}
	if t, ok := m.labels[core.LabelMetricSetType.Key]; ok {
		switch t {
		case core.MetricSetTypePodContainer:
			metaTags["type"] = "container"
		case core.MetricSetTypeSystemContainer:
			metaTags["type"] = "sys-container"
		case core.MetricSetTypePod:
			metaTags["type"] = "pod"
		case core.MetricSetTypeNamespace:
			metaTags["type"] = "namespace"
		case core.MetricSetTypeNode:
			metaTags["type"] = "node"
		case core.MetricSetTypeCluster:
			metaTags["type"] = "cluster"
		default:
			glog.Infof("Unknown metric type %s", t)
		}
	}
	return metaTags
}

func (m *Metric) Value() string {
	switch m.value.ValueType {
	case core.ValueInt64:
		return fmt.Sprintf("%d", m.value.IntValue)
	case core.ValueFloat:
		return fmt.Sprintf("%f", m.value.FloatValue)
	}
	return ""
}

func (m *Metric) Metric(sink *Sink) Carbon2Metric {
	intrinsicTags := m.IntrinsicTags()
	for key, value := range sink.dimensions {
		intrinsicTags[key] = value
	}
	metaTags := m.MetaTags()
	for key, value := range sink.dimensions {
		metaTags[key] = value
	}
	return Carbon2Metric{
		IntrinsicTags:intrinsicTags,
		MetaTags:metaTags,
		Value:m.Value(),
		Timestamp:m.timestamp}
}