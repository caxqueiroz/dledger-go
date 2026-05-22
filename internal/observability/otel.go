package observability

import (
	"cmp"
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Setup installs an OpenTelemetry tracer provider. When OTEL_EXPORTER_OTLP_ENDPOINT
// (or OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) is set, traces are exported via OTLP/HTTP;
// otherwise a provider with no exporter is installed (tests/local dev).
//
// OTEL_EXPORTER_OTLP_INSECURE=true disables TLS for the exporter.
func Setup(ctx context.Context, service string) (shutdown func(context.Context) error, err error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(service)),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}

	endpoint := cmp.Or(
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
		os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
	)
	if endpoint != "" {
		exporterOpts := []otlptracehttp.Option{}
		if os.Getenv("OTEL_EXPORTER_OTLP_INSECURE") == "true" {
			exporterOpts = append(exporterOpts, otlptracehttp.WithInsecure())
		}
		exp, expErr := otlptracehttp.New(ctx, exporterOpts...)
		if expErr != nil {
			return nil, fmt.Errorf("otlp exporter: %w", expErr)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
