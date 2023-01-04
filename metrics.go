package golongpoll

import "github.com/prometheus/client_golang/prometheus"

var EventsInQueue = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "events_in_queue",
		Help: "Number of events in the queue",
	},
	[]string{"category"})
