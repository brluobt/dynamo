# Enhanced Controller Metrics for Reconciliation Performance Observability

**Status**: Draft

**Authors**: [brluo]

**Category**: Architecture

**Replaces**: N/A

**Replaced By**: N/A

**Sponsor**: [TBD - code owner or maintainer]

**Required Reviewers**: [TBD - technical leads]

**Review Date**: [TBD]

**Pull Request**: [TBD]

**Implementation PR / Tracking Issue**: [TBD]

# Summary

Extend the Dynamo Operator's Prometheus metrics framework to provide fine-grained, sub-operation-level observability into every controller's reconciliation loop. The current `ObservedReconciler` wrapper only captures total reconcile duration, total count, and error count. When creating a large number of pods — particularly through the Grove pathway (PodCliqueSet / PodClique) — reconciliation performance degrades significantly due to a combination of cross-controller resource contention and reconcile event storms. The existing metrics provide no visibility into **where** the time is being spent, **why** reconciliations fail, or **how** contention between the Dynamo Operator and Grove Operator manifests at the Kubernetes API level. This proposal introduces a layered metrics instrumentation approach: a general-purpose framework that all controllers opt into, with targeted conflict-storm metrics for the Grove pathway, and extension points for controller-specific metrics to be added incrementally.

# Motivation

During load testing with large-scale pod creation, the operator's reconciliation latency increases significantly. The current metrics (`reconcile_duration_seconds`, `reconcile_total`, `reconcile_errors_total`) tell us **that** reconciliation is slow but not **why**. Two distinct but interrelated problems have been observed:

## Problem 1: No Sub-Operation Visibility

The existing metrics only capture the total duration and outcome of each reconciliation. Specifically:

1. **No sub-operation visibility** – A single DynamoComponentDeployment reconcile touches Deployments, Services, Ingresses, PVCs, HPA, ConfigMaps, and status updates. We cannot determine which of these operations is the bottleneck.

2. **No work queue pressure metrics** – We have no insight into how many reconcile requests are queued and waiting, how long items sit in the queue before being processed, or how often items are requeued.

3. **No Kubernetes API call latency breakdown** – Reconcilers make many API calls (Get, List, Create, Update, Patch, Delete). We cannot distinguish whether latency comes from API server pressure, etcd contention, or controller-side processing.

4. **Missing coverage** – The `CheckpointReconciler` is not wrapped with `ObservedReconciler`, creating a blind spot.

5. **No concurrent reconciliation tracking** – We cannot see how many reconciliations are running in parallel, making it hard to tune `MaxConcurrentReconciles`.

## Problem 2: Cross-Controller Conflict Storm on PodClique/PodCliqueSet

When using the Grove pathway, the DynamoGraphDeployment (DGD) controller and the Grove Operator's own controllers both operate on the same PodCliqueSet (PCS) and PodClique (PC) objects, creating a severe resource contention pattern under load. The object ownership hierarchy is:

```
DynamoGraphDeployment (DGD)                ← Dynamo Operator manages
  └── PodCliqueSet (PCS)                   ← Dynamo Operator creates/updates; Grove Controller also reconciles
        ├── PodClique (PC) - frontend      ← Grove Controller creates/manages; Dynamo Operator reads status and scales
        ├── PodClique (PC) - worker        ← Grove Controller creates/manages; Dynamo Operator reads status and scales
        └── PodCliqueScalingGroup (PCSG)   ← Grove Controller creates/manages (multinode)
              └── PodClique ...
```

The conflict manifests as follows:

1. **Dynamo DGD Controller** creates/updates the PodCliqueSet via `SyncResource()` (Get → Compare hash → Update), directly scales PodClique/PCSG via the Scale subresource, and reads PodClique status to check readiness.

2. **Grove Controllers** reconcile PodCliqueSet to create/manage PodCliques, reconcile PodCliques to manage actual Pods, and continuously update PCS/PC Status fields (replicas, readyReplicas, observedGeneration).

3. **DGD Controller watches PodClique status changes** — when `readyReplicas` or `replicas` changes on any PodClique, the DGD controller is triggered to reconcile via `mapPodCliqueToRequests`.

Under large-scale pod creation, this creates two interrelated performance degradation patterns:

**Pattern A: Watch Amplification and Sequential Wasted Reconciles**

controller-runtime's work queue **deduplicates** items: if the same DGD key is already queued, subsequent enqueue attempts are collapsed. Additionally, `MaxConcurrentReconciles` defaults to 1, so only one DGD reconcile runs at a time. This means we do NOT get 100 concurrent reconciles fighting for the same object. Instead, we get **many rapid sequential reconciles** — each one finishes, immediately another starts because new PC status changes arrived during the previous reconcile.

The key inefficiency: `SyncResource()` compares the PCS spec hash before deciding whether to Update. If the DGD spec hasn't changed (which is the case for watch-triggered reconciles), the hash matches and **PCS Update is skipped**. But the reconcile still executes: it calls `reconcileGroveScaling()` (Scale operations on each PC/PCSG), `GetComponentReadinessAndServiceReplicaStatuses()` (Get on each PC/PCSG), and `r.Status().Update()` (DGD status). Each of these operations costs API calls and time. Most of these reconciles are "wasted" — they confirm what's already true without changing anything.

**Pattern B: Scale and Status Update Conflicts**

While PCS spec Update conflicts are rare (the hash check prevents unnecessary Updates), contention still occurs at:

1. **`reconcileGroveScaling()`** — This uses the Scale subresource (Get scale → compare → Update scale) on each PodClique/PCSG. If Grove's PodClique controller updates the PC status (replicas, readyReplicas) between the Get and the Scale Update, the Dynamo controller receives a 409 Conflict. This is the primary source of actual conflicts.

2. **DGD Status Update** — The defer block calls `r.Status().Update(ctx, dynamoDeployment)`. If the DGD object was modified between the initial Get and the deferred status Update (e.g., by the DGDSA controller syncing replicas), this can also produce a 409 Conflict.

The combined effect:

```
Large batch of PodCliques created
  → Each PC status changes frequently as Pods transition (Pending → Running → Ready)
  → Each PC status change triggers a DGD reconcile (via Watch predicate)
  → Work queue deduplicates, but reconciles execute in rapid sequence
  → Most reconciles find PCS spec unchanged (hash match → skip PCS Update)
  → But each reconcile still executes Scale operations on PC/PCSG
     → Scale operations occasionally hit 409 Conflict (Grove concurrently updating PC status)
     → Failed reconcile is requeued → another reconcile starts immediately
  → Meanwhile, readiness checks and DGD status updates consume API server bandwidth
  → The cumulative overhead of rapid sequential unnecessary reconciles degrades end-to-end deployment latency
```

For example, deploying an inference graph with 5 services × 10 replicas = 50 PodClique Pods. Each Pod transition produces ~2-3 PC status changes. While the work queue collapses simultaneous events, the high rate of status changes ensures a near-continuous stream of sequential DGD reconciles throughout the deployment window. Each reconcile performs O(services) Get + Scale operations, and a fraction of these hit 409 Conflicts on the Scale subresource due to concurrent Grove status updates.

**Currently, there is no way to observe this conflict pattern**: we cannot see the conflict rate, the requeue amplification factor, which sub-operation (PCS sync vs. PC scaling vs. status check) is failing, or how much time is wasted on retried reconciliations.

Without these metrics, diagnosing and resolving performance regressions requires ad-hoc debugging, log analysis, and guesswork. A comprehensive metrics framework would allow operators and developers to pinpoint bottlenecks, quantify contention, set informed alerts, and make data-driven tuning decisions.

## Goals

* Provide a general-purpose metrics instrumentation framework that covers all controllers uniformly
* Add sub-operation timing metrics within reconciliation loops to identify bottlenecks
* Expose controller work queue depth, latency, and retry metrics
* Track Kubernetes API call latency by operation type and resource kind
* Track in-flight (concurrent) reconciliation count per controller
* Quantify cross-controller resource contention (conflict rate, conflict target, requeue amplification) specifically for the Grove PodClique/PodCliqueSet pathway
* Track reconcile trigger sources to distinguish spec-change-driven vs. status-watch-driven reconciliations
* Ensure `CheckpointReconciler` is instrumented consistently with other controllers
* Design extension points so controller-specific metrics can be added incrementally without framework changes
* Update the existing Grafana dashboard with new panels, including a dedicated "Grove Contention" section
* Maintain backward compatibility with all existing metrics

### Non Goals

* This proposal does NOT cover application-level metrics (vLLM/TRT-LLM/SGLang inference metrics)
* This proposal does NOT cover Rust runtime or data-plane metrics in `lib/`
* This proposal does NOT cover metrics internal to the Grove Operator itself (Grove should add its own instrumentation)
* While the conflict analysis focuses on the Grove pathway, the sub-operation and general metrics (Layer 2) apply equally to the non-Grove (DCD) pathway. Contention in the DCD pathway (e.g., multiple DCD reconciles competing for the same Deployment objects) will be captured by the same `sync_resource_conflict_total` metric via `SyncResource` instrumentation
* This proposal does NOT prescribe specific alert thresholds or SLOs (those should be defined per-deployment)
* This proposal does NOT change the reconciliation logic itself; it is purely additive instrumentation. Architectural mitigations for the conflict storm (e.g., watch predicate throttling, SSA adoption) are noted as future work but out of scope
* Detailed per-controller business-logic metrics (e.g. rolling update step counts, profiling job durations) are deferred to future incremental additions

## Requirements

### REQ 1 Universal Controller Coverage

All six controllers **MUST** be wrapped with the metrics-instrumented reconciler:
- DynamoComponentDeploymentReconciler
- DynamoGraphDeploymentReconciler
- DynamoGraphDeploymentScalingAdapterReconciler
- DynamoGraphDeploymentRequestReconciler
- DynamoModelReconciler
- CheckpointReconciler (currently missing)

### REQ 2 Sub-Operation Timing

The framework **MUST** provide a mechanism for recording the duration of named sub-operations within a reconcile loop (e.g., "create_deployment", "update_service", "status_update"). Controllers **SHOULD** be able to use this mechanism with minimal code changes.

### REQ 3 Work Queue Metrics

The framework **MUST** expose per-controller work queue metrics including:
- Queue depth (current number of pending items)
- Queue add rate
- Queue processing latency (time from enqueue to dequeue)
- Retry count

### REQ 4 In-Flight Reconciliation Tracking

The framework **MUST** track the number of currently executing reconciliations per controller to enable capacity planning and `MaxConcurrentReconciles` tuning.

### REQ 5 Kubernetes API Call Metrics

The framework **SHOULD** expose latency histograms for Kubernetes API calls, broken down by verb (Get, List, Create, Update, Patch, Delete) and resource kind.

### REQ 6 Backward Compatibility

All existing metrics (`dynamo_operator_reconcile_duration_seconds`, `dynamo_operator_reconcile_total`, `dynamo_operator_reconcile_errors_total`, `dynamo_operator_resources_total`, webhook metrics) **MUST** continue to function unchanged.

### REQ 7 Conflict and Contention Tracking

The framework **MUST** track Kubernetes API conflict errors (409 Conflict) separately from other errors, with labels identifying the target resource kind (e.g., PodCliqueSet, PodClique). The framework **SHOULD** provide a way to identify the reconcile trigger source (spec change vs. owned-resource status watch) to distinguish necessary reconciliations from status-watch-amplified ones.

### REQ 8 Low Overhead

Metric recording **MUST NOT** add more than 1ms overhead per reconciliation loop on average. Label cardinality **MUST** be bounded and predictable.

### REQ 9 Grafana Dashboard

A Grafana dashboard update **SHOULD** be provided that visualizes the new metrics alongside existing ones, including a dedicated panel section for Grove pathway contention analysis.

# Proposal

## Overview

The proposal introduces four layers of instrumentation:

```
┌─────────────────────────────────────────────────────────┐
│                   Layer 4: Controller-Specific           │
│  (Future: per-controller business logic metrics)        │
├─────────────────────────────────────────────────────────┤
│                   Layer 3: Conflict & Contention         │
│  (sync_resource_conflict_total, reconcile_trigger)      │
│  (Targeted at Grove PCS/PC cross-controller conflict)   │
├─────────────────────────────────────────────────────────┤
│                   Layer 2: Sub-Operation Timing          │
│  (reconcile_sub_operation_duration_seconds)             │
│  (reconcile_inflight, reconcile_requeue_total)          │
├─────────────────────────────────────────────────────────┤
│                   Layer 1: Existing (Enhanced)           │
│  (reconcile_duration_seconds, reconcile_total, etc.)   │
│  (+ CheckpointReconciler coverage)                     │
└─────────────────────────────────────────────────────────┘
```

### Layer 1: Fix Existing Gaps

1. Wrap `CheckpointReconciler` with `ObservedReconciler` in `SetupWithManager()`.
2. Leverage controller-runtime's built-in work queue metrics (already exposed by default through the controller-runtime metrics registry when using the standard workqueue). These are exposed under the `workqueue_*` metric family by controller-runtime. We verify and document them.

### Layer 2: New General-Purpose Metrics

Add the following new metrics to the `observability` package:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dynamo_operator_reconcile_sub_operation_duration_seconds` | Histogram | `resource_type`, `namespace`, `operation` | Duration of individual sub-operations within a reconcile loop |
| `dynamo_operator_reconcile_inflight` | Gauge | `resource_type` | Number of currently executing reconciliations |
| `dynamo_operator_reconcile_queue_depth` | Gauge | `resource_type` | Current depth of the reconcile work queue |
| `dynamo_operator_k8s_api_call_duration_seconds` | Histogram | `resource_type`, `verb`, `target_kind` | Latency of Kubernetes API calls made during reconciliation |
| `dynamo_operator_reconcile_requeue_total` | Counter | `resource_type`, `namespace`, `reason` | Count of requeue events with categorized reasons |

**Label cardinality analysis:**

- `resource_type`: 6 values (bounded by controller count)
- `namespace`: bounded by cluster namespace count (typically < 100)
- `operation`: bounded, explicitly defined per controller (e.g., "get_resource", "create_deployment", "update_service", "update_status")
- `verb`: 6 values (Get, List, Create, Update, Patch, Delete)
- `target_kind`: bounded by Kubernetes resource types the operator manages (~15)
- `reason`: bounded by enum (e.g., "retry", "dependency_not_ready", "rate_limited")

### Layer 3: Conflict and Contention Metrics (Grove Pathway)

This layer targets the specific cross-controller conflict storm observed when the DGD controller and Grove operator simultaneously operate on PodCliqueSet/PodClique objects. New metrics:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `dynamo_operator_sync_resource_conflict_total` | Counter | `resource_type`, `namespace`, `target_kind`, `operation` | Count of 409 Conflict errors in `SyncResource` and Scale operations, broken down by the target object kind |
| `dynamo_operator_reconcile_trigger_total` | Counter | `resource_type`, `namespace`, `trigger` | Count of reconcile triggers by source: `spec_change`, `owned_status_change`, `watch_status_change`, `deletion`, `requeue` |
| `dynamo_operator_reconcile_wasted_total` | Counter | `resource_type`, `namespace` | Count of reconciliations that performed no actual work (the resource was already in the desired state, no sub-resources were modified) — measures the amplification cost of status-watch-driven reconciles |

These metrics together answer the critical questions:
- **How much contention?** `sync_resource_conflict_total` shows the conflict rate and which object kind (PodCliqueSet, PodClique, PCSG) is the bottleneck. This complements the existing `reconcile_errors_total{error_type="conflict"}` metric by adding `target_kind` attribution — the existing metric tells you "the DGD controller had X conflicts" but not "the conflicts were on PodClique Scale vs PodCliqueSet Update vs DGD Status Update".
- **Why so many reconciles?** `reconcile_trigger_total` distinguishes necessary spec-change-driven reconciles from amplified status-watch-driven reconciles.
- **How much waste?** `reconcile_wasted_total` quantifies reconciliations that achieved nothing — pure overhead from the watch amplification loop. A "wasted" reconcile still costs multiple API calls (Get PCS, Get each PC for readiness, Scale calls, Status Update) even if no state changes result.

**Label cardinality budget for Layer 3:**
- `sync_resource_conflict_total`: 6 resource_type × N namespaces × ~5 target_kind × 2 operation = low (conflicts are infrequent events)
- `reconcile_trigger_total`: 6 resource_type × N namespaces × 5 trigger = moderate
- `reconcile_wasted_total`: 6 resource_type × N namespaces = low

Combined with Layer 2 metrics, the estimated total new time series in a 10-namespace cluster: ~5,000–10,000 (dominated by histogram buckets in `reconcile_sub_operation_duration_seconds`). This is well within Prometheus' capacity for operator-level metrics but should be monitored if the operator manages hundreds of namespaces.

### Layer 4: Extension Points for Controller-Specific Metrics

A `MetricsContext` struct is introduced that controllers receive and can use to record sub-operation timings. This is the primary extension point: when a specific controller needs more detailed metrics (e.g., "time spent computing rolling update hash"), it can simply add a new `operation` label value without any framework changes.

## Detailed Design

### New Types and Interfaces

```go
// MetricsContext provides metrics recording capabilities to reconcilers.
// Passed via context to avoid changing reconciler signatures.
type MetricsContext struct {
    resourceType string
    namespace    string
    startTime    time.Time
}

// RecordSubOperation records the duration of a named sub-operation.
func (mc *MetricsContext) RecordSubOperation(operation string, start time.Time) {
    duration := time.Since(start)
    reconcileSubOpDuration.WithLabelValues(
        mc.resourceType, mc.namespace, operation,
    ).Observe(duration.Seconds())
}

// TrackAPICall records a Kubernetes API call with verb and target kind.
func (mc *MetricsContext) TrackAPICall(verb, targetKind string, start time.Time) {
    duration := time.Since(start)
    k8sAPICallDuration.WithLabelValues(
        mc.resourceType, verb, targetKind,
    ).Observe(duration.Seconds())
}
```

### Enhanced ObservedReconciler

```go
func (m *ObservedReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
    // Track in-flight reconciliations
    reconcileInflight.WithLabelValues(m.resourceType).Inc()
    defer reconcileInflight.WithLabelValues(m.resourceType).Dec()

    // Create metrics context and inject into ctx
    mc := &MetricsContext{
        resourceType: m.resourceType,
        namespace:    req.Namespace,
        startTime:    time.Now(),
    }
    ctx = WithMetricsContext(ctx, mc)

    startTime := time.Now()
    result, err := m.Reconciler.Reconcile(ctx, req)
    duration := time.Since(startTime)

    requeue := result.Requeue || result.RequeueAfter > 0

    RecordReconciliation(m.resourceType, req.Namespace, err, requeue, duration)

    // Record requeue reason if applicable
    if requeue && err == nil {
        reconcileRequeueTotal.WithLabelValues(
            m.resourceType, req.Namespace, "explicit_requeue",
        ).Inc()
    } else if err != nil {
        reconcileRequeueTotal.WithLabelValues(
            m.resourceType, req.Namespace, categorizeError(err),
        ).Inc()
    }

    return result, err
}
```

### Usage in Controllers (Example: DynamoComponentDeploymentReconciler)

Controllers opt into sub-operation metrics by retrieving `MetricsContext` from the context:

```go
func (r *DynamoComponentDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, err error) {
    mc := observability.MetricsContextFromContext(ctx)

    // ... existing Get() call ...

    // Track sub-operation: reconcile workload resources
    subOpStart := time.Now()
    componentReconcileResult, err = r.reconcileDeploymentResources(ctx, dcd)
    if mc != nil {
        mc.RecordSubOperation("reconcile_workload", subOpStart)
    }

    // Track sub-operation: reconcile services
    subOpStart = time.Now()
    serviceModified, err := r.createOrUpdateOrDeleteServices(ctx, opts)
    if mc != nil {
        mc.RecordSubOperation("reconcile_services", subOpStart)
    }

    // Track sub-operation: reconcile ingress
    subOpStart = time.Now()
    ingressModified, err := r.createOrUpdateOrDeleteIngress(ctx, opts)
    if mc != nil {
        mc.RecordSubOperation("reconcile_ingress", subOpStart)
    }

    // Track sub-operation: update status
    subOpStart = time.Now()
    err = r.setStatusConditionAndServiceReplicaStatus(ctx, dcd, result)
    if mc != nil {
        mc.RecordSubOperation("update_status", subOpStart)
    }

    // ...
}
```

### Conflict Tracking in SyncResource

The `SyncResource` generic function in `controller_common/resource.go` is the primary site where 409 Conflict errors occur. Add conflict recording at the Update call site:

```go
func SyncResource[T client.Object](ctx context.Context, r Reconciler, parentResource client.Object, generateResource ResourceGenerator[T]) (modified bool, res T, err error) {
    // ... existing Get and Compare logic ...

    err = r.Update(ctx, oldResource)
    if err != nil {
        // Record conflict metric if this is a 409 Conflict
        if k8serrors.IsConflict(err) {
            mc := observability.MetricsContextFromContext(ctx)
            if mc != nil {
                mc.RecordConflict(resourceType, "update")
            }
        }
        return
    }
    // ...
}
```

Similarly, instrument the Scale subresource calls in `scaleGroveResource`:

```go
func (r *DynamoGraphDeploymentReconciler) scaleGroveResource(ctx context.Context, resourceName, namespace string, newReplicas int32, resourceType string) error {
    // ... existing scaling logic ...
    _, err = r.ScaleClient.Scales(namespace).Update(ctx, gvr.GroupResource(), scale, metav1.UpdateOptions{})
    if err != nil {
        if k8serrors.IsConflict(err) {
            mc := observability.MetricsContextFromContext(ctx)
            if mc != nil {
                mc.RecordConflict(resourceType, "scale")
            }
        }
    }
    return err
}
```

### Reconcile Trigger Source Tracking

Add trigger attribution in the DGD controller's `SetupWithManager()` Watch predicate and in the `ObservedReconciler`. The trigger is injected into the context via a reconcile annotation:

```go
// In mapPodCliqueToRequests — tag the request so the reconciler knows the trigger source
func (r *DynamoGraphDeploymentReconciler) mapPodCliqueToRequests(ctx context.Context, obj client.Object) []ctrl.Request {
    podClique, ok := obj.(*grovev1alpha1.PodClique)
    if !ok {
        return nil
    }
    dgdName, hasLabel := podClique.GetLabels()[consts.KubeLabelDynamoGraphDeploymentName]
    if !hasLabel || dgdName == "" {
        return nil
    }
    req := ctrl.Request{NamespacedName: types.NamespacedName{Name: dgdName, Namespace: podClique.Namespace}}
    // Trigger source annotation is tracked by the ObservedReconciler via a context key
    // set by the event source mapper
    return []ctrl.Request{req}
}
```

The `ObservedReconciler` then records:

```go
reconcileTriggerTotal.WithLabelValues(m.resourceType, req.Namespace, trigger).Inc()
```

Where `trigger` is one of: `spec_change`, `owned_status_change`, `watch_status_change`, `deletion`, `requeue`.

> **Implementation note:** controller-runtime does not natively support passing metadata from Watch event handlers to the Reconcile function. The simplest approach is to infer the trigger inside the Reconcile function: if `DGD.Generation == DGD.Status.ObservedGeneration` (no spec change since last successful reconcile), classify as `watch_status_change`; otherwise classify as `spec_change`.
>
> **Known limitation:** `ObservedGeneration` is only updated on successful reconciliation. If the previous reconcile failed (e.g., due to a 409 Conflict), `ObservedGeneration` is stale and the next retry will be misclassified as `spec_change` instead of `requeue`. Mitigation options:
> 1. Track last-seen generation in the `ObservedReconciler` wrapper (in-memory, lost on restart — acceptable for metrics)
> 2. Accept the imprecision and document that `spec_change` count includes retries after failed reconciles
> 3. Use a separate in-memory requeue tracker that marks keys as "pending retry"
>
> The exact approach will be determined during implementation.

### Wasted Reconcile Tracking

A reconciliation is "wasted" when no sub-resources were modified and the status was already in the desired state. The DGD controller already tracks this via the `modified` boolean pattern:

```go
// In reconcileGroveResources:
// If SyncResource reports no modification AND all components are already ready,
// the reconcile was triggered by a status change that did not require any action.
if !modified && allComponentsReady {
    mc.RecordWastedReconcile()
}
```

### Work Queue Metrics

controller-runtime (v0.22.4) already exposes standard workqueue metrics through the Prometheus registry. These include:

- `workqueue_depth` – current queue depth
- `workqueue_adds_total` – total adds
- `workqueue_queue_duration_seconds` – time items spend in queue before processing
- `workqueue_work_duration_seconds` – time spent processing items
- `workqueue_retries_total` – total retries
- `workqueue_unfinished_work_seconds` – unfinished work time
- `workqueue_longest_running_processor_seconds` – longest running processor

These are labeled by `name` which corresponds to the controller name set in `Named()`. Since all our controllers set explicit names via `consts.ResourceType*`, these are already partitioned per controller.

**Action:** Verify these metrics are exposed, document them, and add them to the Grafana dashboard. If for any reason they are not available, add a custom `dynamo_operator_reconcile_queue_depth` gauge populated from a periodic sampler (similar to `resource_counter.go`).

### Kubernetes API Client Metrics

controller-runtime provides client-go metrics via `k8s.io/client-go/tools/metrics`. These are registered by default:

- `rest_client_requests_total` – total API requests by verb and status code
- `rest_client_request_duration_seconds` – request latency by verb and URL

**Action:** Verify these are exposed and document them. For operator-specific context (which controller triggered the API call), use the `MetricsContext.TrackAPICall()` helper at key call sites.

### File Changes Summary

| File | Change |
|------|--------|
| `internal/observability/metrics.go` | Add new metric definitions: Layer 2 (`reconcileSubOpDuration`, `reconcileInflight`, `reconcileRequeueTotal`, `k8sAPICallDuration`) and Layer 3 (`syncResourceConflictTotal`, `reconcileTriggerTotal`, `reconcileWastedTotal`) |
| `internal/observability/reconciler_wrapper.go` | Enhance `ObservedReconciler.Reconcile()` with inflight tracking, `MetricsContext` injection, and trigger classification |
| `internal/observability/metrics_context.go` | New file: `MetricsContext` struct with `RecordSubOperation`, `TrackAPICall`, `RecordConflict`, `RecordWastedReconcile`, and context helpers |
| `internal/controller/dynamocheckpoint_controller.go` | Wrap with `ObservedReconciler` in `SetupWithManager()` |
| `internal/controller/dynamographdeployment_controller.go` | (Phase 2) Add sub-operation instrumentation, conflict tracking in `reconcileGrovePodCliqueSet`, `reconcileGroveScaling`, `reconcileGroveResources`; trigger classification; wasted-reconcile detection |
| `internal/controller/*_controller.go` | (Phase 2) Add `mc.RecordSubOperation()` calls at key points in all other controllers |
| `internal/controller_common/resource.go` | (Phase 2) Add conflict recording in `SyncResource` on 409 Conflict errors |
| `deploy/observability/grafana_dashboards/dynamo-operator.json` | Add new dashboard panels including "Grove Contention" section |
| `deploy/observability/k8s/grafana-operator-dashboard-configmap.yaml` | Update ConfigMap with new dashboard |
| `docs/kubernetes/observability/operator-metrics.md` | Document all new metrics including contention analysis guide |

# Implementation Details

## Metric Definitions

```go
var (
    // Layer 2: Sub-operation and general metrics
    reconcileSubOpDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: metricsNamespace,
            Name:      "reconcile_sub_operation_duration_seconds",
            Help:      "Duration of individual sub-operations within a reconcile loop",
            Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
        },
        []string{"resource_type", "namespace", "operation"},
    )

    reconcileInflight = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{
            Namespace: metricsNamespace,
            Name:      "reconcile_inflight",
            Help:      "Number of reconciliations currently in progress",
        },
        []string{"resource_type"},
    )

    reconcileRequeueTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: metricsNamespace,
            Name:      "reconcile_requeue_total",
            Help:      "Total number of reconciliation requeue events",
        },
        []string{"resource_type", "namespace", "reason"},
    )

    k8sAPICallDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Namespace: metricsNamespace,
            Name:      "k8s_api_call_duration_seconds",
            Help:      "Duration of Kubernetes API calls during reconciliation",
            Buckets:   []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
        },
        []string{"resource_type", "verb", "target_kind"},
    )

    // Layer 3: Conflict and contention metrics
    syncResourceConflictTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: metricsNamespace,
            Name:      "sync_resource_conflict_total",
            Help:      "Total 409 Conflict errors when syncing or scaling resources, indicating cross-controller contention",
        },
        []string{"resource_type", "namespace", "target_kind", "operation"},
    )

    reconcileTriggerTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: metricsNamespace,
            Name:      "reconcile_trigger_total",
            Help:      "Total reconcile triggers by source (spec_change, watch_status_change, deletion, requeue)",
        },
        []string{"resource_type", "namespace", "trigger"},
    )

    reconcileWastedTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Namespace: metricsNamespace,
            Name:      "reconcile_wasted_total",
            Help:      "Total reconciliations that performed no work (resource already in desired state)",
        },
        []string{"resource_type", "namespace"},
    )
)
```

## MetricsContext Implementation

```go
package observability

import (
    "context"
    "time"
)

type contextKey struct{}

type MetricsContext struct {
    resourceType string
    namespace    string
    startTime    time.Time
}

func NewMetricsContext(resourceType, namespace string) *MetricsContext {
    return &MetricsContext{
        resourceType: resourceType,
        namespace:    namespace,
        startTime:    time.Now(),
    }
}

func WithMetricsContext(ctx context.Context, mc *MetricsContext) context.Context {
    return context.WithValue(ctx, contextKey{}, mc)
}

func MetricsContextFromContext(ctx context.Context) *MetricsContext {
    mc, _ := ctx.Value(contextKey{}).(*MetricsContext)
    return mc
}

func (mc *MetricsContext) RecordSubOperation(operation string, start time.Time) {
    if mc == nil {
        return
    }
    reconcileSubOpDuration.WithLabelValues(
        mc.resourceType, mc.namespace, operation,
    ).Observe(time.Since(start).Seconds())
}

func (mc *MetricsContext) TrackAPICall(verb, targetKind string, start time.Time) {
    if mc == nil {
        return
    }
    k8sAPICallDuration.WithLabelValues(
        mc.resourceType, verb, targetKind,
    ).Observe(time.Since(start).Seconds())
}

func (mc *MetricsContext) RecordConflict(targetKind, operation string) {
    if mc == nil {
        return
    }
    syncResourceConflictTotal.WithLabelValues(
        mc.resourceType, mc.namespace, targetKind, operation,
    ).Inc()
}

func (mc *MetricsContext) RecordWastedReconcile() {
    if mc == nil {
        return
    }
    reconcileWastedTotal.WithLabelValues(
        mc.resourceType, mc.namespace,
    ).Inc()
}
```

## CheckpointReconciler Fix

```go
// In dynamocheckpoint_controller.go SetupWithManager():
func (r *CheckpointReconciler) SetupWithManager(mgr ctrl.Manager) error {
    return ctrl.NewControllerManagedBy(mgr).
        For(&nvidiacomv1alpha1.DynamoCheckpoint{}).
        Owns(&batchv1.Job{}, builder.WithPredicates(predicate.Funcs{
            CreateFunc:  func(ce event.CreateEvent) bool { return false },
            DeleteFunc:  func(de event.DeleteEvent) bool { return true },
            UpdateFunc:  func(ue event.UpdateEvent) bool { return true },
            GenericFunc: func(ge event.GenericEvent) bool { return true },
        })).
        WithEventFilter(commonController.EphemeralDeploymentEventFilter(r.Config, r.RuntimeConfig)).
        Complete(observability.NewObservedReconciler(r, consts.ResourceTypeDynamoCheckpoint))
}
```

## Example PromQL Queries

### General Performance

```promql
# Sub-operation P95 latency for DynamoComponentDeployment
histogram_quantile(0.95,
  sum by (operation, le) (
    rate(dynamo_operator_reconcile_sub_operation_duration_seconds_bucket{
      resource_type="DynamoComponentDeployment"
    }[5m])
  )
)

# Currently in-flight reconciliations
dynamo_operator_reconcile_inflight

# Work queue depth per controller (built-in controller-runtime metric)
workqueue_depth{name=~"DynamoGraphDeployment|DynamoComponentDeployment|.*"}

# Work queue wait time P99
histogram_quantile(0.99,
  sum by (name, le) (
    rate(workqueue_queue_duration_seconds_bucket[5m])
  )
)

# Requeue rate by reason
sum by (resource_type, reason) (
  rate(dynamo_operator_reconcile_requeue_total[5m])
)

# K8s API call latency by verb and target kind
histogram_quantile(0.95,
  sum by (verb, target_kind, le) (
    rate(dynamo_operator_k8s_api_call_duration_seconds_bucket[5m])
  )
)

# Reconciliation throughput vs queue depth (saturation indicator)
sum by (resource_type) (rate(dynamo_operator_reconcile_total[5m]))
/
(dynamo_operator_reconcile_inflight > 0)
```

### Grove Contention Analysis

```promql
# Conflict rate by target kind — is PodCliqueSet or PodClique the bottleneck?
sum by (target_kind, operation) (
  rate(dynamo_operator_sync_resource_conflict_total{
    resource_type="DynamoGraphDeployment"
  }[5m])
)

# Conflict-to-success ratio — what percentage of resource syncs hit conflicts?
sum(rate(dynamo_operator_sync_resource_conflict_total[5m]))
/
sum(rate(dynamo_operator_reconcile_total{resource_type="DynamoGraphDeployment"}[5m]))

# Reconcile trigger breakdown — how many reconciles are spec-driven vs watch-amplified?
sum by (trigger) (
  rate(dynamo_operator_reconcile_trigger_total{
    resource_type="DynamoGraphDeployment"
  }[5m])
)

# Watch amplification ratio — watch-triggered reconciles vs spec-triggered
sum(rate(dynamo_operator_reconcile_trigger_total{trigger="watch_status_change"}[5m]))
/
sum(rate(dynamo_operator_reconcile_trigger_total{trigger="spec_change"}[5m]))

# Wasted reconcile rate — what percentage of reconciles do no useful work?
sum(rate(dynamo_operator_reconcile_wasted_total{resource_type="DynamoGraphDeployment"}[5m]))
/
sum(rate(dynamo_operator_reconcile_total{resource_type="DynamoGraphDeployment"}[5m]))

# Sub-operation latency breakdown for Grove pathway
histogram_quantile(0.95,
  sum by (operation, le) (
    rate(dynamo_operator_reconcile_sub_operation_duration_seconds_bucket{
      resource_type="DynamoGraphDeployment",
      operation=~"sync_pcs|scale_pc|scale_pcsg|check_readiness"
    }[5m])
  )
)
```

## Deferred to Implementation

- Exact set of `operation` label values per controller will be finalized during code review
- Whether to introduce a helper macro/wrapper for `RecordSubOperation` to reduce boilerplate
- Dashboard panel layout and thresholds
- Whether `target_kind` in `k8s_api_call_duration_seconds` should use GVK or just Kind
- Precise mechanism for trigger source attribution (generation comparison vs. in-memory tracking vs. context annotation from event handler); see Implementation note on `reconcile_trigger_total` for trade-offs
- Whether `reconcile_wasted_total` should be a label on `reconcile_total` instead of a separate counter
- Whether to add a `pathway` label (`grove` vs `component`) to DGD controller metrics to distinguish performance characteristics between the two deployment modes

## Future Work (Out of Scope)

The metrics introduced by this proposal will provide data-driven evidence for future architectural optimizations to address the conflict storm root cause. These are explicitly out of scope but noted for reference:

1. **Watch predicate throttling** – Instead of triggering a DGD reconcile on every PodClique status change, batch status changes using a time-based or threshold-based predicate (e.g., only trigger when all PCs are ready, or at most once per N seconds). The `reconcile_trigger_total` and `reconcile_wasted_total` metrics will quantify the benefit of this optimization.

2. **Server-Side Apply (SSA) for PodCliqueSet** – Replace the Get → Compare → Update pattern in `SyncResource` with SSA (`Patch` with `ApplyConfiguration`), which eliminates `resourceVersion` conflicts entirely by using field ownership. The `sync_resource_conflict_total` metric will quantify the conflict rate that SSA would eliminate.

3. **DGD controller rate limiter tuning** – Configure per-controller rate limiters (`MaxConcurrentReconciles`, `RateLimiter`) based on observed `reconcile_inflight` and `workqueue_depth` data.

4. **Grove-side instrumentation** – Recommend the Grove team add similar metrics to their PodCliqueSet and PodClique controllers to provide the other half of the contention picture.

# Implementation Phases

## Phase 0: Fix Coverage Gap and Verify Built-in Metrics

**Release Target**: Immediate (1-2 days)

**Effort Estimate**: Small

**Work Item(s):** Single PR

**Supported API / Behavior:**

* Wrap `CheckpointReconciler` with `ObservedReconciler`
* Verify and document controller-runtime built-in workqueue metrics (`workqueue_*`)
* Verify and document client-go REST client metrics (`rest_client_*`)
* Update `operator-metrics.md` documentation

**Not Supported:**

* No new custom metrics yet

## Phase 1: Core Metrics Framework

**Release Target**: 1-2 weeks after Phase 0

**Effort Estimate**: Medium

**Work Item(s):** 1-2 PRs

**Supported API / Behavior:**

* New metric definitions in `metrics.go`
* `MetricsContext` type with context injection in `reconciler_wrapper.go`
* `reconcile_inflight` gauge tracking
* `reconcile_requeue_total` counter
* Registration in `InitMetrics()`
* Unit tests for new metrics

**Not Supported:**

* Sub-operation instrumentation in controllers (Phase 2)
* Grafana dashboard updates (Phase 3)

## Phase 2: Controller Sub-Operation Instrumentation and Conflict Tracking

**Release Target**: 1-2 weeks after Phase 1

**Effort Estimate**: Medium-Large

**Work Item(s):** Per-controller PRs (can be parallelized)

**Supported API / Behavior:**

* Add `mc.RecordSubOperation()` calls to all six controllers at major sub-operation boundaries
* Add `mc.TrackAPICall()` at key API call sites (optional, based on review)
* Add conflict recording (`mc.RecordConflict()`) in `SyncResource` and `scaleGroveResource` on 409 Conflict errors
* Add trigger classification (`reconcile_trigger_total`) in DGD controller using generation comparison
* Add wasted-reconcile detection (`reconcile_wasted_total`) in DGD controller's Grove pathway
* Suggested initial sub-operations per controller:

| Controller | Sub-Operations |
|------------|----------------|
| DynamoComponentDeployment | `get_resource`, `handle_finalizer`, `reconcile_workload`, `reconcile_services`, `reconcile_model_services`, `reconcile_ingress`, `update_status` |
| DynamoGraphDeployment | `get_resource`, `handle_finalizer`, `rolling_update_check`, `reconcile_pvcs`, `reconcile_checkpoints`, `reconcile_scaling_adapters`, `reconcile_epp`, `sync_pcs` (PodCliqueSet sync), `scale_pc` (PodClique scale), `scale_pcsg` (PCSG scale), `check_readiness`, `reconcile_model_services`, `update_status` |
| DynamoGraphDeploymentScalingAdapter | `get_resource`, `get_dgd`, `sync_replicas`, `update_status` |
| DynamoGraphDeploymentRequest | `get_resource`, `handle_finalizer`, `phase_transition`, `create_job`, `check_job`, `create_dgd`, `update_status` |
| DynamoModel | `get_resource`, `handle_finalizer`, `discover_endpoints`, `aggregate_endpoints`, `update_status` |
| Checkpoint | `get_resource`, `handle_finalizer`, `create_job`, `check_job`, `update_status` |

Note: The DGD controller has additional Grove-specific sub-operations (`sync_pcs`, `scale_pc`, `scale_pcsg`, `check_readiness`) that are critical for diagnosing the cross-controller contention issue.

**Not Supported:**

* Fine-grained business logic metrics (future Phase 3+ additions)

## Phase 3: Grafana Dashboard and Documentation

**Release Target**: 1 week after Phase 2

**Effort Estimate**: Small-Medium

**Work Item(s):** 1 PR

**Supported API / Behavior:**

* New Grafana dashboard panels:
  - Sub-operation duration heatmap per controller
  - In-flight reconciliation gauge
  - Work queue depth and latency
  - Requeue rate by reason
  - K8s API call latency breakdown
  - **Grove Contention section**: Conflict rate by target kind, watch amplification ratio, wasted reconcile percentage, trigger source breakdown
* Updated `operator-metrics.md` with all new metrics, including a contention analysis guide
* Example alert rules (informational, not enforced):
  - `dynamo_operator_sync_resource_conflict_total` rate > threshold → cross-controller contention alert
  - Wasted reconcile ratio > 50% → watch amplification alert

**Not Supported:**

* SLO definitions or alert policies (deployment-specific)

# Related Proposals

* N/A (this is the first metrics-focused DEP)

# Alternate Solutions

## Alt 1: Structured Logging with Log Aggregation

**Pros:**

* No code changes to metrics infrastructure
* Flexible querying via log aggregation tools (Loki, ELK)
* Can capture arbitrary context

**Cons:**

* Higher storage cost at scale
* Slower to query than Prometheus
* No built-in alerting integration
* Requires separate log parsing/indexing pipeline

**Reason Rejected:**

Prometheus metrics are the established pattern in the Kubernetes ecosystem for operational monitoring. The operator already has a mature metrics pipeline (ServiceMonitor, Grafana dashboard). Structured logging is complementary but not a replacement for time-series metrics needed for dashboards and alerts.

## Alt 2: OpenTelemetry Tracing

**Pros:**

* Rich, per-request trace context with spans for each sub-operation
* Distributed tracing across operator → API server → etcd
* Excellent for debugging individual slow reconciliations

**Cons:**

* Significant additional infrastructure (Jaeger/Tempo collector, storage)
* Higher per-request overhead than Prometheus counters/histograms
* Not yet standard in the controller-runtime ecosystem
* Overkill for aggregate performance monitoring

**Reason Rejected:**

OpenTelemetry tracing is a valuable future addition but requires significant infrastructure investment. Prometheus metrics provide the aggregate view needed for dashboards, alerts, and capacity planning with minimal overhead. Tracing can be added later for deep-dive debugging.

## Alt 3: Custom Middleware Wrapping client.Client

**Pros:**

* Automatically instruments all API calls without per-controller changes
* Single point of instrumentation

**Cons:**

* Loses controller context (which controller triggered the call)
* Complex to implement correctly (must handle all client.Client methods)
* client-go already provides REST client metrics

**Reason Rejected:**

The `MetricsContext` via Go context approach provides controller attribution while being simpler to implement. Combined with the existing `rest_client_*` metrics from client-go, this provides sufficient API call visibility.

# Background

## Current Metrics Architecture

The Dynamo Operator uses controller-runtime v0.22.4 with Prometheus metrics exposed via the built-in metrics server. The existing `observability` package (`deploy/operator/internal/observability/`) contains:

- `metrics.go` – Metric definitions and recording functions
- `reconciler_wrapper.go` – `ObservedReconciler` wrapper for reconcile-level metrics
- `resource_counter.go` – Background goroutine for resource inventory gauges
- `webhook_wrapper.go` – Webhook admission metrics

Five of six controllers are wrapped with `ObservedReconciler`; `CheckpointReconciler` is the exception.

## Controller Architecture

All controllers follow the standard controller-runtime pattern:
1. Get the primary resource
2. Handle finalizer (create/delete)
3. Reconcile sub-resources (Deployments, Services, PVCs, etc.)
4. Update status conditions
5. Return result (success, error, or requeue)

The `DynamoComponentDeploymentReconciler` and `DynamoGraphDeploymentReconciler` are the most complex, managing multiple sub-resources and having the longest reconciliation paths.

## Grove Pathway and Cross-Controller Interaction

When the Grove orchestrator is enabled, the DGD controller takes the "Grove pathway" (`isGrovePathway() == true`) instead of creating DynamoComponentDeployments. In this pathway:

1. **DGD Controller** creates a PodCliqueSet via `reconcileGrovePodCliqueSet()` using `SyncResource()` (Get → hash compare → Update).
2. **Grove's PodCliqueSet Controller** reconciles the PCS and creates PodClique and PodCliqueScalingGroup objects.
3. **Grove's PodClique Controller** creates and manages actual Pods, continuously updating PC Status (`replicas`, `readyReplicas`, `updatedReplicas`).
4. **DGD Controller watches PodClique** via `mapPodCliqueToRequests()` — any change to `readyReplicas` or `replicas` triggers a new DGD reconcile.
5. **DGD Controller also scales** PodClique/PCSG directly via the Scale subresource in `reconcileGroveScaling()`.

This creates a shared-mutable-state pattern: the PodCliqueSet object is written by both the Dynamo DGD controller (spec updates) and the Grove PCS controller (status updates), while PodClique objects are written by Grove (status) and read/scaled by Dynamo. Under Kubernetes' optimistic concurrency model, concurrent writes to the same object result in 409 Conflict errors for the slower writer.

The watch on PodClique status creates an amplification effect: each Pod state transition generates a PC status update, which triggers a DGD reconcile enqueue. controller-runtime's work queue **deduplicates** items by key — if a DGD is already queued, subsequent triggers are collapsed. With `MaxConcurrentReconciles=1` (the default), at most one DGD reconcile executes at a time with one pending. However, the high event rate ensures a near-continuous stream of sequential reconciles: each reconcile completes, immediately another starts because new events arrived during its execution.

Within each reconcile, `SyncResource` on PCS typically detects no spec change (hash match) and skips the Update. But `reconcileGroveScaling()` executes Scale operations on each PC/PCSG, and `GetComponentReadinessAndServiceReplicaStatuses()` performs Get operations on each PC/PCSG for readiness checking. The Scale operations are the primary site for 409 Conflict errors — Grove concurrently updates PC status between the Scale Get and Scale Update. Even without conflicts, the cumulative cost of these API calls across many rapid sequential reconciles degrades throughput and increases end-to-end deployment time.

## References

* [Prometheus Best Practices for Histograms](https://prometheus.io/docs/practices/histograms/)
* [controller-runtime Metrics](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/metrics)
* [Kubernetes Metrics Stability Framework](https://kubernetes.io/docs/concepts/cluster-administration/system-metrics/)
* [client-go Metrics](https://github.com/kubernetes/client-go/blob/master/tools/metrics/metrics.go)

## Terminology & Definitions

| Term | Definition |
| :---- | :---- |
| **DCD** | DynamoComponentDeployment – represents a single component deployment |
| **DGD** | DynamoGraphDeployment – represents an inference graph deployment |
| **DGDSA** | DynamoGraphDeploymentScalingAdapter – bridges external scalers to DGD |
| **DGDR** | DynamoGraphDeploymentRequest – profiling-to-deployment workflow |
| **PCS** | PodCliqueSet – Grove CRD representing a set of PodCliques that form an inference graph |
| **PC** | PodClique – Grove CRD representing a group of co-scheduled Pods for a single service |
| **PCSG** | PodCliqueScalingGroup – Grove CRD for managing multiple PodClique replicas (multinode) |
| **Sub-operation** | A discrete step within a reconciliation loop (e.g., creating a Deployment) |
| **Work queue** | controller-runtime's internal queue of objects pending reconciliation |
| **Conflict storm** | A positive feedback loop where concurrent writes to the same Kubernetes object produce cascading 409 Conflict errors and requeue amplification |
| **Watch amplification** | The effect of Watch predicates triggering many reconciliations from frequent status changes, most of which do no useful work |
| **Wasted reconcile** | A reconciliation triggered by a Watch event that results in no state changes (resource already in desired state) |
| **Optimistic concurrency** | Kubernetes' mechanism where every object has a `resourceVersion`; an Update must match the current version or receive a 409 Conflict |

## Acronyms & Abbreviations

**DEP:** Dynamo Enhancement Proposal

**DCD:** DynamoComponentDeployment

**DGD:** DynamoGraphDeployment

**DGDSA:** DynamoGraphDeploymentScalingAdapter

**DGDR:** DynamoGraphDeploymentRequest

**PCS:** PodCliqueSet

**PC:** PodClique

**PCSG:** PodCliqueScalingGroup

**SSA:** Server-Side Apply

**HPA:** Horizontal Pod Autoscaler

**PVC:** Persistent Volume Claim

**GVK:** Group Version Kind
