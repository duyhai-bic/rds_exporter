package performance_insights

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type performanceInsightsMetrics struct {
	AlignedStartTime time.Time `json:"AlignedStartTime" help:"The start time for the returned metrics, after alignment to a granular boundary."`
	AlignedEndTime   time.Time `json:"AlignedEndTime" help:"The end time for the returned metrics, after alignment to a granular boundary."`
	Identifier       string    `json:"Identifier" help:"ResourceID of the RDS instance."`
	MetricList       []metric  `json:"MetricList" help:"An array of metric results, where each array element contains all of the data points for a particular dimension."`
}

type metric struct {
	Key        metricKey         `json:"Key" help:"The dimensions to which the data points apply."`
	DataPoints []metricDataPoint `json:"DataPoints" help:"A timestamp, and a single numerical value, which together represent a measurement at a particular point in time."`
}

type metricKey struct {
	Metric     string            `json:"Metric" help:"The name of a Performance Insights metric to be measured."`
	Dimensions map[string]string `json:"Dimensions" help:"The valid dimensions for the metric."`
}

type metricDataPoint struct {
	Timestamp time.Time `json:"Timestamp" help:"The time, in epoch format, associated with a particular Value."`
	Value     float64   `json:"Value" help:"The actual value associated with a particular Timestamp."`
}

// parses performance insights metrics from given JSON data.
func parsePerformanceInsightsMetrics(b []byte, disallowUnknownFields bool) (*performanceInsightsMetrics, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	if disallowUnknownFields {
		d.DisallowUnknownFields()
	}

	var m performanceInsightsMetrics
	if err := d.Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// makeGauge returns Prometheus gauge for given reflect.Value.
func makeGauge(desc *prometheus.Desc, labelValues []string, value reflect.Value) prometheus.Metric {
	// skip nil fields
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}

	var f float64
	switch kind := value.Kind(); kind {
	case reflect.Float64:
		f = value.Float()
	case reflect.Int, reflect.Int64:
		f = float64(value.Int())
	default:
		panic(fmt.Errorf("can't make a metric value for %s from %v (%s)", desc, value, kind))
	}

	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, f, labelValues...)
}

// makeMetric makes metrics for all structure fields.
func makeMetric(m *metric, constLabels prometheus.Labels) prometheus.Metric {
	namePrefix := "rds_pi_"
	piMetricName := strings.ReplaceAll(m.Key.Metric, ".", "_")
	metricName := namePrefix + piMetricName

	// build variable labels
	dimensionLen := len(m.Key.Dimensions)
	labelKeys := make([]string, 0, dimensionLen)
	labelValues := make([]string, 0, dimensionLen)
	for k, v := range m.Key.Dimensions {
		normalizedKey := strings.ReplaceAll(k, ".", "_")
		labelKeys = append(labelKeys, normalizedKey)
		labelValues = append(labelValues, v)
	}

	// build prometheus metric
	help := m.Key.Metric + " metric from RDS Performance Insights"
	desc := prometheus.NewDesc(metricName, help, labelKeys, constLabels)
	dataPoint := m.DataPoints[0]
	v := reflect.ValueOf(dataPoint.Value)
	pm := makeGauge(desc, labelValues, v)
	return pm
}

// makePrometheusMetrics returns all Prometheus metrics for given performance insights.
func (m *performanceInsightsMetrics) makePrometheusMetrics(instanceIdentifier string, region string, labels map[string]string) []prometheus.Metric {
	res := make([]prometheus.Metric, 0, 100)

	constLabels := prometheus.Labels{
		"region":              region,
		"instance_identifier": instanceIdentifier,
	}
	for n, v := range labels {
		if v == "" {
			delete(constLabels, n)
		} else {
			constLabels[n] = v
		}
	}

	for _, mt := range m.MetricList {
		met := makeMetric(&mt, constLabels)
		res = append(res, met)
	}

	return res
}
