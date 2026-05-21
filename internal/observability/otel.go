package observability

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Setup wires a no-op tracer by default. If OTEL_EXPORTER_OTLP_ENDPOINT is set,
// callers can swap in an OTLP exporter. We keep the dependency surface small.
func Setup(_ context.Context, service string) (shutdown func(context.Context) error, err error) {
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	_ = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") // hook point for follow-up wiring
	_ = service
	return tp.Shutdown, nil
}
