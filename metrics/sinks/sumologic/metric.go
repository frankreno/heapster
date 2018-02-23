package sumologic

import (
	"bytes"
	"fmt"
)

type Carbon2Metric struct {
	IntrinsicTags 	map[string]string
	MetaTags 		map[string]string
	Value     		string
	Timestamp 		int64
}

func NewCarbon2Metric(intrinsicTags map[string]string, metaTags map[string]string, value string, timestamp int64) Carbon2Metric {
	return Carbon2Metric{
		IntrinsicTags: 	intrinsicTags,
		MetaTags: 		metaTags,
		Value:     		value,
		Timestamp: 		timestamp,
	}
}

func (metric Carbon2Metric) String() string {
	var buffer bytes.Buffer
	for k, v := range metric.IntrinsicTags {
		buffer.WriteString(fmt.Sprintf("%s=%s ", k, v))
	}
	buffer.WriteString(" ")
	for k, v := range metric.MetaTags {
		buffer.WriteString(fmt.Sprintf("%s=%s ", k, v))
	}
	buffer.WriteString(fmt.Sprintf("%s ", metric.Value))
	buffer.WriteString(fmt.Sprintf("%d", metric.Timestamp))
	return buffer.String()
}