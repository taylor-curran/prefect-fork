from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional
from uuid import UUID

from opentelemetry import trace
from opentelemetry.trace import Span, Status, StatusCode, get_tracer

import prefect
import prefect.settings

if TYPE_CHECKING:
    from opentelemetry.trace import Tracer

    from prefect.workers.base import BaseWorker


@dataclass
class WorkerTelemetry:
    """
    A class for managing the telemetry of worker health and lifecycle events.
    """

    _tracer: "Tracer" = field(
        default_factory=lambda: get_tracer("prefect.worker", prefect.__version__)
    )
    worker_span: Span | None = None
    heartbeat_span: Span | None = None
    polling_span: Span | None = None
    _enabled: bool = field(
        default_factory=lambda: prefect.settings.get_current_settings().cloud.enable_orchestration_telemetry
    )

    def start_worker_span(self, worker: "BaseWorker") -> Span | None:
        """Start a span for worker lifecycle."""
        if not self._enabled:
            return None

        self.worker_span = self._tracer.start_span(
            name=f"worker.{worker.type}",
            attributes={
                "worker.name": worker.name,
                "worker.type": worker.type,
                "worker.work_pool_name": worker._work_pool_name,
                "worker.heartbeat_interval_seconds": worker.heartbeat_interval_seconds,
                "worker.prefetch_seconds": getattr(worker, "prefetch_seconds", None),
                "worker.limit": getattr(worker, "limit", None),
            },
        )
        return self.worker_span

    def start_heartbeat_span(self, worker: "BaseWorker") -> Span | None:
        """Start a span for worker heartbeat."""
        if not self._enabled:
            return None

        context = (
            trace.set_span_in_context(self.worker_span) if self.worker_span else None
        )
        self.heartbeat_span = self._tracer.start_span(
            name="worker.heartbeat",
            context=context,
            attributes={
                "worker.name": worker.name,
                "worker.work_pool_name": worker._work_pool_name,
            },
        )
        return self.heartbeat_span

    def end_heartbeat_span(
        self,
        success: bool,
        worker_id: Optional[UUID] = None,
        error: Optional[Exception] = None,
    ) -> None:
        """End heartbeat span with success/failure status."""
        if self.heartbeat_span:
            if success:
                self.heartbeat_span.set_status(Status(StatusCode.OK))
                if worker_id:
                    self.heartbeat_span.set_attribute(
                        "worker.backend_id", str(worker_id)
                    )
            else:
                self.heartbeat_span.set_status(
                    Status(StatusCode.ERROR, "Heartbeat failed")
                )
                if error:
                    self.heartbeat_span.record_exception(error)

            self.heartbeat_span.end(time.time_ns())
            self.heartbeat_span = None

    def start_polling_span(self, worker: "BaseWorker") -> Span | None:
        """Start a span for work queue polling."""
        if not self._enabled:
            return None

        context = (
            trace.set_span_in_context(self.worker_span) if self.worker_span else None
        )
        self.polling_span = self._tracer.start_span(
            name="worker.poll_work_queue",
            context=context,
            attributes={
                "worker.name": worker.name,
                "worker.work_pool_name": worker._work_pool_name,
            },
        )
        return self.polling_span

    def end_polling_span(
        self, flow_runs_count: int, error: Optional[Exception] = None
    ) -> None:
        """End polling span with flow run count and status."""
        if self.polling_span:
            self.polling_span.set_attribute(
                "worker.flow_runs_retrieved", flow_runs_count
            )

            if error:
                self.polling_span.set_status(Status(StatusCode.ERROR, "Polling failed"))
                self.polling_span.record_exception(error)
            else:
                self.polling_span.set_status(Status(StatusCode.OK))

            self.polling_span.end(time.time_ns())
            self.polling_span = None

    def record_health_check(
        self,
        is_healthy: bool,
        query_interval_seconds: float,
        seconds_since_last_poll: int,
    ) -> None:
        """Record a health check event."""
        if not self._enabled or not self.worker_span:
            return

        self.worker_span.add_event(
            "worker.health_check",
            {
                "worker.health.is_healthy": is_healthy,
                "worker.health.query_interval_seconds": query_interval_seconds,
                "worker.health.seconds_since_last_poll": seconds_since_last_poll,
            },
        )

    def end_worker_span(self, error: Optional[Exception] = None) -> None:
        """End worker lifecycle span."""
        if self.worker_span:
            if error:
                self.worker_span.set_status(
                    Status(StatusCode.ERROR, "Worker stopped with error")
                )
                self.worker_span.record_exception(error)
            else:
                self.worker_span.set_status(Status(StatusCode.OK))

            self.worker_span.end(time.time_ns())
            self.worker_span = None
