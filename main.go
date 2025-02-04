package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/common/version"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/duyhai-bic/rds_exporter/client"
	"github.com/duyhai-bic/rds_exporter/config"
	"github.com/duyhai-bic/rds_exporter/enhanced"
	"github.com/duyhai-bic/rds_exporter/sessions"
)

//nolint:lll
var (
	listenAddressF           = kingpin.Flag("web.listen-address", "Address on which to expose metrics and web interface.").Default(":9042").String()
	basicMetricsPathF        = kingpin.Flag("web.basic-telemetry-path", "Path under which to expose exporter's basic metrics.").Default("/basic").String()
	enhancedMetricsPathF     = kingpin.Flag("web.enhanced-telemetry-path", "Path under which to expose exporter's enhanced metrics.").Default("/enhanced").String()
	performanceInsightsPathF = kingpin.Flag("web.performance-insights-telemetry-path", "Path under which to expose exporter's performance insights metrics.").Default("/performance-insights").String()
	configFileF              = kingpin.Flag("config.file", "Path to configuration file.").Default("config.yml").String()
	logTraceF                = kingpin.Flag("log.trace", "Enable verbose tracing of AWS requests (will log credentials).").Default("false").Bool()
	logger                   = log.NewNopLogger()
)

func initSession(configFileF *string, client *client.Client, logger log.Logger, logTraceF *bool) (*config.Config, *sessions.Sessions) {
	cfg, err := config.Load(*configFileF)
	if err != nil {
		level.Error(logger).Log("msg", "Can't read configuration file", "error", err)
		os.Exit(1)
	}

	sess, err := sessions.New(cfg.Instances, client.HTTP(), logger, *logTraceF)
	if err != nil {
		level.Error(logger).Log("msg", "Can't create sessions", "error", err)
		os.Exit(1)
	}
	return cfg, sess
}

func main() {
	kingpin.HelpFlag.Short('h')
	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Version(version.Print("rds_exporter"))
	kingpin.Parse()
	logger = promlog.New(promlogConfig)
	level.Info(logger).Log("msg", fmt.Sprintf("Starting RDS exporter %s", version.Info()))
	level.Info(logger).Log("msg", fmt.Sprintf("Build context %s", version.BuildContext()))

	client := client.New(logger)

	_, sess := initSession(configFileF, client, logger, logTraceF)

	// Disable cloudwatch metrics, as we will use YACE for all CW metrics
	// basic metrics + client metrics + exporter own metrics (ProcessCollector and GoCollector)
	// basicCollector := basic.New(cfg, sess, logger)
	// {
	// 	prometheus.MustRegister(basicCollector)
	// 	prometheus.MustRegister(client)
	// 	http.Handle(*basicMetricsPathF, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
	// 		//ErrorLog:      log.NewErrorLogger(), TODO TS
	// 		ErrorHandling: promhttp.ContinueOnError,
	// 	}))
	// }

	// enhanced metrics
	enhancedCollector := enhanced.NewCollector(sess, logger)
	{
		registry := prometheus.NewRegistry()
		registry.MustRegister(enhancedCollector)
		http.Handle(*enhancedMetricsPathF, promhttp.HandlerFor(registry, promhttp.HandlerOpts{
			//ErrorLog:      log.NewErrorLogger(), TODO TS
			ErrorHandling: promhttp.ContinueOnError,
		}))
	}

	// Periodically reinitialize the AWS session (and update collectors) – e.g. every hour.
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		for range ticker.C {
			level.Info(logger).Log("msg", "Periodic reinitialization of AWS session and configuration")
			_, sess := initSession(configFileF, client, logger, logTraceF)
			// Disable cloudwatch metrics, as we will use YACE for all CW metrics
			// basicCollector.Update(cfg, sess)
			enhancedCollector.Update(sess, logger)
		}
	}()

	// level.Info(logger).Log("msg", fmt.Sprintf("Basic metrics   : http://%s%s", *listenAddressF, *basicMetricsPathF))
	level.Info(logger).Log("msg", fmt.Sprintf("Enhanced metrics: http://%s%s", *listenAddressF, *enhancedMetricsPathF))

	level.Error(logger).Log("error", http.ListenAndServe(*listenAddressF, nil))
}
